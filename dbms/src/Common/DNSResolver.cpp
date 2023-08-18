// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/DNSPTRResolverProvider.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Common/NetException.h>
#include <Common/ProfileEvents.h>
#include <Core/Names.h>
#include <Poco/Exception.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberFormatter.h>
#include <Poco/NumberParser.h>
#include <arpa/inet.h>
#include <common/types.h>
#include <fmt/core.h>

#include <atomic>
#include <optional>
#include <string_view>
#include <unordered_set>

namespace ProfileEvents
{
extern const Event DNSError;
}

namespace std
{
template <>
struct hash<Poco::Net::IPAddress>
{
    size_t operator()(const Poco::Net::IPAddress & address) const noexcept
    {
        std::string_view addr(static_cast<const char *>(address.addr()), address.length());
        std::hash<std::string_view> hash_impl;
        return hash_impl(addr);
    }
};
} // namespace std

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int DNS_ERROR;
} // namespace ErrorCodes

namespace
{

/// Slightly altered implementation from https://github.com/pocoproject/poco/blob/poco-1.6.1/Net/src/SocketAddress.cpp#L86
void splitHostAndPort(const std::string & host_and_port, std::string & out_host, UInt16 & out_port)
{
    String port_str;
    out_host.clear();

    auto it = host_and_port.begin();
    auto end = host_and_port.end();

    if (*it == '[') /// Try parse case '[<IPv6 or something else>]:<port>'
    {
        ++it;
        while (it != end && *it != ']')
            out_host += *it++;
        if (it == end)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Malformed IPv6 address");
        ++it;
    }
    else /// Case '<IPv4 or domain name or something else>:<port>'
    {
        while (it != end && *it != ':')
            out_host += *it++;
    }

    if (it != end && *it == ':')
    {
        ++it;
        while (it != end)
            port_str += *it++;
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing port number");

    unsigned port;
    if (Poco::NumberParser::tryParseUnsigned(port_str, port) && port <= 0xFFFF)
    {
        out_port = static_cast<UInt16>(port);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Port must be numeric");
}

///
/// getaddrinfo_a failed on CentOS, use `gethostbyname` instead.
///

void DNSOnError(int code, const std::string & arg)
{
    switch (code)
    {
    case POCO_ESYSNOTREADY:
        throw Poco::Net::NetException("Net subsystem not ready");
    case POCO_ENOTINIT:
        throw Poco::Net::NetException("Net subsystem not initialized");
    case POCO_HOST_NOT_FOUND:
        throw Poco::Net::HostNotFoundException(arg);
    case POCO_TRY_AGAIN:
        throw Poco::Net::DNSException("Temporary DNS error while resolving", arg);
    case POCO_NO_RECOVERY:
        throw Poco::Net::DNSException("Non recoverable DNS error while resolving", arg);
    case POCO_NO_DATA:
        throw Poco::Net::NoAddressFoundException(arg);
    default:
        throw Poco::IOException(Poco::NumberFormatter::format(code));
    }
}
Poco::Net::HostEntry DNShostByName(const std::string & hostname)
{
    struct hostent * he = gethostbyname(hostname.c_str());
    if (he)
    {
        return Poco::Net::HostEntry(he);
    }
    DNSOnError(h_errno, hostname); // will throw an appropriate exception
    throw Poco::Net::NetException(); // to silence compiler
}

DNSResolver::IPAddresses hostByName(const std::string & host)
{
    DNSResolver::IPAddresses addresses;

    try
    {
        addresses = DNShostByName(host).addresses();
    }
    catch (const Poco::Net::DNSException & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("DNSResolver"),
            "Cannot resolve host ({}), error {}: {}.",
            host,
            e.code(),
            e.name());
        addresses.clear();
    }
    catch (Poco::IOException & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("DNSResolver"),
            "Cannot resolve host ({}), io error {}: {}.",
            host,
            e.code(),
            e.name());
        addresses.clear();
    }

    if (addresses.empty())
    {
        ProfileEvents::increment(ProfileEvents::DNSError);
        throw Exception(ErrorCodes::DNS_ERROR, "Not found address of host: {}", host);
    }

    return addresses;
}

DNSResolver::IPAddresses resolveIPAddressImpl(const std::string & host)
{
    Poco::Net::IPAddress ip;

    /// NOTE:
    /// - Poco::Net::DNS::resolveOne(host) doesn't work for IP addresses like 127.0.0.2
    /// - Poco::Net::IPAddress::tryParse() expect hex string for IPv6 (without brackets)
    if (host.starts_with('['))
    {
        assert(host.ends_with(']'));
        if (Poco::Net::IPAddress::tryParse(host.substr(1, host.size() - 2), ip))
            return DNSResolver::IPAddresses(1, ip);
    }
    else
    {
        if (Poco::Net::IPAddress::tryParse(host, ip))
            return DNSResolver::IPAddresses(1, ip);
    }

    DNSResolver::IPAddresses addresses = hostByName(host);

    return addresses;
}

DNSResolver::IPAddresses resolveIPAddressWithCache(
    LRUCache<std::string, DNSResolver::IPAddresses> & cache,
    const std::string & host)
{
    auto [result, _] = cache.getOrSet(host, [&host]() {
        return std::make_shared<DNSResolver::IPAddresses>(resolveIPAddressImpl(host));
    });
    return *result;
}

std::unordered_set<String> reverseResolveImpl(const Poco::Net::IPAddress & address)
{
    auto ptr_resolver = DB::DNSPTRResolverProvider::get();

    if (address.family() == Poco::Net::IPAddress::Family::IPv4)
    {
        return ptr_resolver->resolve(address.toString());
    }
    else
    {
        return ptr_resolver->resolve_v6(address.toString());
    }
}

std::unordered_set<String> reverseResolveWithCache(
    LRUCache<Poco::Net::IPAddress, std::unordered_set<std::string>> & cache,
    const Poco::Net::IPAddress & address)
{
    auto [result, _] = cache.getOrSet(address, [&address]() {
        return std::make_shared<std::unordered_set<String>>(reverseResolveImpl(address));
    });
    return *result;
}

Poco::Net::IPAddress pickAddress(const DNSResolver::IPAddresses & addresses)
{
    return addresses.front();
}

} // namespace

struct DNSResolver::Impl
{
    using HostWithConsecutiveFailures = std::unordered_map<String, UInt32>;
    using AddressWithConsecutiveFailures = std::unordered_map<Poco::Net::IPAddress, UInt32>;

    LRUCache<std::string, DNSResolver::IPAddresses> cache_host{100};
    LRUCache<Poco::Net::IPAddress, std::unordered_set<std::string>> cache_address{100};

    std::mutex drop_mutex;
    std::mutex update_mutex;

    /// Cached server host name
    std::optional<String> host_name;

    /// Store hosts, which was asked to resolve from last update of DNS cache.
    HostWithConsecutiveFailures new_hosts;
    AddressWithConsecutiveFailures new_addresses;

    /// Store all hosts, which was whenever asked to resolve
    HostWithConsecutiveFailures known_hosts;
    AddressWithConsecutiveFailures known_addresses;

    /// If disabled, will not make cache lookups, will resolve addresses manually on each call
    std::atomic<bool> disable_cache{false};
};


DNSResolver::DNSResolver()
    : impl(std::make_unique<DNSResolver::Impl>())
    , log(Logger::get("DNSResolver"))
{}

Poco::Net::IPAddress DNSResolver::resolveHost(const std::string & host)
{
    return pickAddress(resolveHostAll(host));
}

DNSResolver::IPAddresses DNSResolver::resolveHostAll(const std::string & host)
{
    if (impl->disable_cache)
        return resolveIPAddressImpl(host);

    addToNewHosts(host);
    return resolveIPAddressWithCache(impl->cache_host, host);
}

Poco::Net::SocketAddress DNSResolver::resolveAddress(const std::string & host_and_port)
{
    if (impl->disable_cache)
        return Poco::Net::SocketAddress(host_and_port);

    String host;
    UInt16 port;
    splitHostAndPort(host_and_port, host, port);

    addToNewHosts(host);
    return Poco::Net::SocketAddress(pickAddress(resolveIPAddressWithCache(impl->cache_host, host)), port);
}

Poco::Net::SocketAddress DNSResolver::resolveAddress(const std::string & host, UInt16 port)
{
    if (impl->disable_cache)
        return Poco::Net::SocketAddress(host, port);

    addToNewHosts(host);
    return Poco::Net::SocketAddress(pickAddress(resolveIPAddressWithCache(impl->cache_host, host)), port);
}

std::vector<Poco::Net::SocketAddress> DNSResolver::resolveAddressList(const std::string & host, UInt16 port)
{
    if (Poco::Net::IPAddress ip; Poco::Net::IPAddress::tryParse(host, ip))
        return std::vector<Poco::Net::SocketAddress>{{ip, port}};

    std::vector<Poco::Net::SocketAddress> addresses;

    if (!impl->disable_cache)
        addToNewHosts(host);

    std::vector<Poco::Net::IPAddress> ips
        = impl->disable_cache ? hostByName(host) : resolveIPAddressWithCache(impl->cache_host, host);
    auto ips_end = std::unique(ips.begin(), ips.end());

    addresses.reserve(ips_end - ips.begin());
    for (auto ip = ips.begin(); ip != ips_end; ++ip)
        addresses.emplace_back(*ip, port);

    return addresses;
}

std::unordered_set<String> DNSResolver::reverseResolve(const Poco::Net::IPAddress & address)
{
    if (impl->disable_cache)
        return reverseResolveImpl(address);

    addToNewAddresses(address);
    return reverseResolveWithCache(impl->cache_address, address);
}

void DNSResolver::dropCache()
{
    impl->cache_host.reset();
    impl->cache_address.reset();

    std::scoped_lock lock(impl->update_mutex, impl->drop_mutex);

    impl->known_hosts.clear();
    impl->known_addresses.clear();
    impl->new_hosts.clear();
    impl->new_addresses.clear();
    impl->host_name.reset();
}

void DNSResolver::removeHostFromCache(const std::string & host)
{
    impl->cache_host.remove(host);
}

void DNSResolver::setDisableCacheFlag(bool is_disabled)
{
    impl->disable_cache = is_disabled;
}

String DNSResolver::getHostName()
{
    if (impl->disable_cache)
        return Poco::Net::DNS::hostName();

    std::lock_guard lock(impl->drop_mutex);

    if (!impl->host_name.has_value())
        impl->host_name.emplace(Poco::Net::DNS::hostName());

    return *impl->host_name;
}

static const String & cacheElemToString(const String & str)
{
    return str;
}
static String cacheElemToString(const Poco::Net::IPAddress & addr)
{
    return addr.toString();
}

template <typename UpdateF, typename ElemsT>
bool DNSResolver::updateCacheImpl(
    UpdateF && update_func,
    ElemsT && elems,
    UInt32 max_consecutive_failures,
    const String & notfound_log_msg,
    const String & dropped_log_msg)
{
    bool updated = false;
    String lost_elems;
    using iterators = typename std::remove_reference_t<decltype(elems)>::iterator;
    std::vector<iterators> elements_to_drop;
    for (auto it = elems.begin(); it != elems.end(); it++)
    {
        try
        {
            updated |= (this->*update_func)(it->first);
            it->second = 0;
        }
        catch (const DB::Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                continue;
            }
            if (!lost_elems.empty())
                lost_elems += ", ";
            lost_elems += cacheElemToString(it->first);
            if (max_consecutive_failures)
            {
                it->second++;
                if (it->second >= max_consecutive_failures)
                    elements_to_drop.emplace_back(it);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    if (!lost_elems.empty())
        LOG_INFO(log, fmt::format(fmt::runtime(notfound_log_msg), lost_elems));
    if (elements_to_drop.size())
    {
        updated = true;
        String deleted_elements;
        for (auto it : elements_to_drop)
        {
            if (!deleted_elements.empty())
                deleted_elements += ", ";
            deleted_elements += cacheElemToString(it->first);
            elems.erase(it);
        }
        LOG_INFO(log, fmt::format(fmt::runtime(dropped_log_msg), deleted_elements));
    }

    return updated;
}

bool DNSResolver::updateCache(UInt32 max_consecutive_failures)
{
    LOG_DEBUG(log, "Updating DNS cache");

    {
        String updated_host_name = Poco::Net::DNS::hostName();

        std::lock_guard lock(impl->drop_mutex);

        for (const auto & host : impl->new_hosts)
            impl->known_hosts.insert(host);
        impl->new_hosts.clear();

        for (const auto & address : impl->new_addresses)
            impl->known_addresses.insert(address);
        impl->new_addresses.clear();

        impl->host_name.emplace(updated_host_name);
    }

    /// FIXME Updating may take a long time because we cannot manage timeouts of getaddrinfo(...) and getnameinfo(...).
    /// DROP DNS CACHE will wait on update_mutex (possibly while holding drop_mutex)
    std::lock_guard lock(impl->update_mutex);

    bool hosts_updated = updateCacheImpl(
        &DNSResolver::updateHost,
        impl->known_hosts,
        max_consecutive_failures,
        "Cached hosts not found: {}",
        "Cached hosts dropped: {}");
    updateCacheImpl(
        &DNSResolver::updateAddress,
        impl->known_addresses,
        max_consecutive_failures,
        "Cached addresses not found: {}",
        "Cached addresses dropped: {}");

    LOG_DEBUG(log, "Updated DNS cache");
    return hosts_updated;
}

bool DNSResolver::updateHost(const String & host)
{
    const auto old_value = resolveIPAddressWithCache(impl->cache_host, host);
    auto new_value = resolveIPAddressImpl(host);
    const bool result = old_value != new_value;
    impl->cache_host.set(host, std::make_shared<DNSResolver::IPAddresses>(std::move(new_value)));
    return result;
}

bool DNSResolver::updateAddress(const Poco::Net::IPAddress & address)
{
    const auto old_value = reverseResolveWithCache(impl->cache_address, address);
    auto new_value = reverseResolveImpl(address);
    const bool result = old_value != new_value;
    impl->cache_address.set(address, std::make_shared<std::unordered_set<String>>(std::move(new_value)));
    return result;
}

void DNSResolver::addToNewHosts(const String & host)
{
    std::lock_guard lock(impl->drop_mutex);
    UInt8 consecutive_failures = 0;
    impl->new_hosts.insert({host, consecutive_failures});
}

void DNSResolver::addToNewAddresses(const Poco::Net::IPAddress & address)
{
    std::lock_guard lock(impl->drop_mutex);
    UInt8 consecutive_failures = 0;
    impl->new_addresses.insert({address, consecutive_failures});
}

DNSResolver::~DNSResolver() = default;

DNSResolver & DNSResolver::instance()
{
    static DNSResolver ret;
    return ret;
}

} // namespace DB

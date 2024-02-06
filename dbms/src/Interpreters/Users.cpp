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

#include <Common/Exception.h>
#include <Common/SimpleCache.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/Buffer/HexWriteBuffer.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Quota.h>
#include <Interpreters/Users.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/RegularExpression.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <common/logger_useful.h>
#include <openssl/sha.h>
#include <string.h>

#include <ext/scope_guard.h>


namespace DB
{
namespace ErrorCodes
{
extern const int DNS_ERROR;
extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
extern const int UNKNOWN_USER;
extern const int REQUIRED_PASSWORD;
extern const int WRONG_PASSWORD;
extern const int IP_ADDRESS_NOT_ALLOWED;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


static Poco::Net::IPAddress toIPv6(const Poco::Net::IPAddress addr)
{
    if (addr.family() == Poco::Net::IPAddress::IPv6)
        return addr;

    return Poco::Net::IPAddress("::FFFF:" + addr.toString());
}


/// IP-address or subnet mask. Example: 213.180.204.3 or 10.0.0.1/8 or 2a02:6b8::3 or 2a02:6b8::3/64.
class IPAddressPattern : public IAddressPattern
{
private:
    /// Address of mask. Always transformed to IPv6.
    Poco::Net::IPAddress mask_address;
    /// Number of bits in mask.
    UInt8 prefix_bits{};

public:
    explicit IPAddressPattern(const String & str)
    {
        const char * pos = strchr(str.c_str(), '/');

        if (nullptr == pos)
        {
            construct(Poco::Net::IPAddress(str));
        }
        else
        {
            String addr(str, 0, pos - str.c_str());
            auto prefix_bits = parse<UInt8>(pos + 1);

            construct(Poco::Net::IPAddress(addr), prefix_bits);
        }
    }

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        return prefixBitsEquals(
            reinterpret_cast<const char *>(toIPv6(addr).addr()),
            reinterpret_cast<const char *>(mask_address.addr()),
            prefix_bits);
    }

private:
    void construct(const Poco::Net::IPAddress & mask_address_)
    {
        mask_address = toIPv6(mask_address_);
        prefix_bits = 128;
    }

    void construct(const Poco::Net::IPAddress & mask_address_, UInt8 prefix_bits_)
    {
        mask_address = toIPv6(mask_address_);
        prefix_bits = mask_address_.family() == Poco::Net::IPAddress::IPv4 ? prefix_bits_ + 96 : prefix_bits_;
    }

    static bool prefixBitsEquals(const char * lhs, const char * rhs, UInt8 prefix_bits)
    {
        UInt8 prefix_bytes = prefix_bits / 8;
        UInt8 remaining_bits = prefix_bits % 8;

        return 0 == memcmp(lhs, rhs, prefix_bytes)
            && (remaining_bits % 8 == 0
                || (lhs[prefix_bytes] >> (8 - remaining_bits)) == (rhs[prefix_bytes] >> (8 - remaining_bits)));
    }
};


/// Check that address equals to one of hostname addresses.
class HostExactPattern : public IAddressPattern
{
private:
    String host;

    static bool containsImpl(const String & host, const Poco::Net::IPAddress & addr)
    {
        Poco::Net::IPAddress addr_v6 = toIPv6(addr);

        /// Resolve by hand, because Poco don't use AI_ALL flag but we need it.
        addrinfo * ai = nullptr;

        addrinfo hints{};
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_flags |= AI_V4MAPPED | AI_ALL;

        int ret = getaddrinfo(host.c_str(), nullptr, &hints, &ai);
        if (0 != ret)
            throw Exception("Cannot getaddrinfo: " + std::string(gai_strerror(ret)), ErrorCodes::DNS_ERROR);

        SCOPE_EXIT({ freeaddrinfo(ai); });

        for (; ai != nullptr; ai = ai->ai_next)
        {
            if (ai->ai_addrlen && ai->ai_addr)
            {
                if (ai->ai_family == AF_INET6)
                {
                    if (addr_v6
                        == Poco::Net::IPAddress(
                            &reinterpret_cast<sockaddr_in6 *>(ai->ai_addr)->sin6_addr,
                            sizeof(in6_addr),
                            reinterpret_cast<sockaddr_in6 *>(ai->ai_addr)->sin6_scope_id))
                    {
                        return true;
                    }
                }
                else if (ai->ai_family == AF_INET)
                {
                    if (addr_v6
                        == toIPv6(Poco::Net::IPAddress(
                            &reinterpret_cast<sockaddr_in *>(ai->ai_addr)->sin_addr,
                            sizeof(in_addr))))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

public:
    explicit HostExactPattern(const String & host_)
        : host(host_)
    {}

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        static SimpleCache<decltype(containsImpl), &containsImpl> cache;
        return cache(host, addr);
    }
};


/// Check that PTR record for address match the regexp (and in addition, check that PTR record is resolved back to client address).
class HostRegexpPattern : public IAddressPattern
{
private:
    Poco::RegularExpression host_regexp;

    static String getDomain(const Poco::Net::IPAddress & addr)
    {
        Poco::Net::SocketAddress sock_addr(addr, 0);

        /// Resolve by hand, because Poco library doesn't have such functionality.
        char domain[1024];
        int gai_errno
            = getnameinfo(sock_addr.addr(), sock_addr.length(), domain, sizeof(domain), nullptr, 0, NI_NAMEREQD);
        if (0 != gai_errno)
            throw Exception("Cannot getnameinfo: " + std::string(gai_strerror(gai_errno)), ErrorCodes::DNS_ERROR);

        return domain;
    }

public:
    explicit HostRegexpPattern(const String & host_regexp_)
        : host_regexp(host_regexp_)
    {}

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        static SimpleCache<decltype(getDomain), &getDomain> cache;

        String domain = cache(addr);
        Poco::RegularExpression::Match match;

        return host_regexp.match(domain, match) && HostExactPattern(domain).contains(addr);
    }
};


bool AddressPatterns::contains(const Poco::Net::IPAddress & addr) const
{
    for (const auto & pattern : patterns)
    {
        /// If host cannot be resolved, skip it and try next.
        try
        {
            if (pattern->contains(addr))
                return true;
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(
                &Poco::Logger::get("AddressPatterns"),
                "Failed to check if pattern contains address {}. {}, code = {}",
                addr.toString(),
                e.displayText(),
                e.code());

            if (e.code() == ErrorCodes::DNS_ERROR)
            {
                continue;
            }
            else
                throw;
        }
    }

    return false;
}

void AddressPatterns::addFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (auto & config_key : config_keys)
    {
        Container::value_type pattern;
        String value = config.getString(config_elem + "." + config_key);

        if (startsWith(config_key, "ip"))
            pattern = std::make_unique<IPAddressPattern>(value);
        else if (startsWith(config_key, "host_regexp"))
            pattern = std::make_unique<HostRegexpPattern>(value);
        else if (startsWith(config_key, "host"))
            pattern = std::make_unique<HostExactPattern>(value);
        else
            throw Exception("Unknown address pattern type: " + config_key, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);

        patterns.emplace_back(std::move(pattern));
    }
}

const User & User::getDefaultUser()
{
    static User u(User::DEFAULT_USER_NAME);
    return u;
}

User::User(const String & name_)
    : name(name_)
    , profile(User::DEFAULT_USER_NAME)
    , quota(QuotaForInterval::DEFAULT_QUOTA_NAME)
{}

User::User(const String & name_, const String & config_elem, Poco::Util::AbstractConfiguration & config)
    : name(name_)
{
    // Allow empty "password" for TiFlash
    bool has_password = config.has(config_elem + ".password");
    bool has_password_sha256_hex = config.has(config_elem + ".password_sha256_hex");

    if (has_password && has_password_sha256_hex)
        throw Exception(
            "Both fields 'password' and 'password_sha256_hex' are specified for user " + name
                + ". Must be only one of them.",
            ErrorCodes::BAD_ARGUMENTS);

    if (has_password)
        password = config.getString(config_elem + ".password");

    if (has_password_sha256_hex)
    {
        password_sha256_hex = Poco::toLower(config.getString(config_elem + ".password_sha256_hex"));

        if (password_sha256_hex.size() != 64)
            throw Exception(
                "password_sha256_hex for user " + name + " has length " + toString(password_sha256_hex.size())
                    + " but must be exactly 64 symbols.",
                ErrorCodes::BAD_ARGUMENTS);
    }

    profile = config.getString(config_elem + ".profile");
    quota = config.getString(config_elem + ".quota");

    addresses.addFromConfig(config_elem + ".networks", config);

    /// Fill list of allowed databases.
    const auto config_sub_elem = config_elem + ".allow_databases";
    if (config.has(config_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_sub_elem, config_keys);

        databases.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config.getString(config_sub_elem + "." + key);
            databases.insert(database_name);
        }
    }
}


} // namespace DB

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

#pragma once
#include <Common/nocopyable.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#include <ext/singleton.h>
#include <memory>


namespace DB
{
/// A singleton implementing global and permanent DNS cache
/// It could be updated only manually via drop() method
class DNSCache : public ext::Singleton<DNSCache>
{
public:
    DISALLOW_COPY(DNSCache);

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolve its IP
    Poco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolve its IP and port
    Poco::Net::SocketAddress resolveHostAndPort(const std::string & host_and_port);

    /// Drops all caches
    void drop();

    ~DNSCache();

protected:
    DNSCache();

    friend class ext::Singleton<DNSCache>;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace DB

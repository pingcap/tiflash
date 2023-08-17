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

#include <Common/isLocalAddress.h>
#include <Core/Types.h>
#include <Poco/Net/NetworkInterface.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/Application.h>

#include <cstring>


namespace DB
{
bool isLocalAddress(const Poco::Net::SocketAddress & address)
{
    static auto interfaces = Poco::Net::NetworkInterface::list();

    return interfaces.end()
        != std::find_if(interfaces.begin(), interfaces.end(), [&](const Poco::Net::NetworkInterface & interface) {
               /** Compare the addresses without taking into account `scope`.
                      * Theoretically, this may not be correct - depends on `route` setting
                      *  - through which interface we will actually access the specified address.
                      */
               return interface.address().length() == address.host().length()
                   && 0 == memcmp(interface.address().addr(), address.host().addr(), address.host().length());
           });
}

bool isLocalAddress(const Poco::Net::SocketAddress & address, UInt16 clickhouse_port)
{
    return clickhouse_port == address.port() && isLocalAddress(address);
}


size_t getHostNameDifference(const std::string & local_hostname, const std::string & host)
{
    size_t hostname_difference = 0;
    for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
        if (local_hostname[i] != host[i])
            ++hostname_difference;
    return hostname_difference;
}

} // namespace DB

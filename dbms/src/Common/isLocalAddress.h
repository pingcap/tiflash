// Copyright 2022 PingCAP, Ltd.
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

#include <common/types.h>

namespace Poco
{
namespace Net
{
class SocketAddress;
}
} // namespace Poco

namespace DB
{
/** Lets you check if the address is similar to `localhost`.
     * The purpose of this check is usually to make an assumption,
     *  that when we go to this address via the Internet, we'll get to ourselves.
     * Please note that this check is not accurate:
     * - the address is simply compared to the addresses of the network interfaces;
     * - only the first address is taken for each network interface;
     * - the routing rules that affect which network interface we go to the specified address are not checked.
     */
bool isLocalAddress(const Poco::Net::SocketAddress & address, UInt16 clickhouse_port);

bool isLocalAddress(const Poco::Net::SocketAddress & address);

/// Returns number of different bytes in hostnames, used for load balancing
size_t getHostNameDifference(const std::string & local_hostname, const std::string & host);
} // namespace DB

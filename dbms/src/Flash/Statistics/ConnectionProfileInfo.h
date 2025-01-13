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

#include <common/defines.h>
#include <common/types.h>

#include <magic_enum.hpp>

namespace DB
{

struct ConnectionProfileInfo
{
    enum ConnectionType
    {
        Local = 0,
        InnerZoneRemote = 1,
        InterZoneRemote = 2,
    };
    using ConnTypeVec = std::vector<ConnectionProfileInfo::ConnectionType>;
    static ALWAYS_INLINE ConnectionType inferConnectionType(bool is_local, bool same_zone)
    {
        if (is_local)
        {
            return Local;
        }
        else if (same_zone)
        {
            return InnerZoneRemote;
        }
        else
        {
            return InterZoneRemote;
        }
    }
    ConnectionProfileInfo() = default;
    explicit ConnectionProfileInfo(ConnectionType type_)
        : type(type_)
    {}
    ConnectionProfileInfo(bool is_local, bool same_zone)
        : ConnectionProfileInfo(inferConnectionType(is_local, same_zone))
    {}

    String getTypeString() const { return String(magic_enum::enum_name(type)); }

    Int64 packets = 0;
    Int64 bytes = 0;
    ConnectionType type = Local;
};
} // namespace DB

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

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/Remote/Proto/common.pb.h>
#include <common/types.h>

template <class T>
constexpr std::false_type always_false{};

namespace DB::PS::V3::Remote
{

// FIXME: The "toRemote" and "fromRemote" name is confusing. Maybe "toRemoteMsg" and "fromRemoteMsg"?

template <typename PageIdType>
PageId toRemote(const PageIdType & id)
{
    PageId remote_id;

    if constexpr (std::is_same_v<DB::UInt128, PageIdType>)
    {
        remote_id.mutable_u128()->set_low(id.low);
        remote_id.mutable_u128()->set_high(id.high);
    }
    else if constexpr (std::is_same_v<DB::UniversalPageId, PageIdType>)
    {
        remote_id.set_str(id.asStr());
    }
    else
    {
        static_assert(always_false<PageIdType>);
    }
    return remote_id;
}

template <typename PageIdType>
PageIdType fromRemote(const PageId & remote_id)
{
    PageIdType id;
    if constexpr (std::is_same_v<DB::UInt128, PageIdType>)
    {
        id.low = remote_id.u128().low();
        id.high = remote_id.u128().high();
    }
    else if constexpr (std::is_same_v<DB::UniversalPageId, PageIdType>)
    {
        id = remote_id.str();
    }
    else
    {
        static_assert(always_false<PageIdType>);
    }
    return id;
}

} // namespace DB::PS::V3::Remote

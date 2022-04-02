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

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <pingcap/kv/RegionCache.h>
#pragma GCC diagnostic pop

namespace DB
{
using RegionVerID = pingcap::kv::RegionVerID;

class LockException : public Exception
{
public:
    explicit LockException(RegionID region_id_, LockInfoPtr lock_info)
        : region_id(region_id_)
        , lock_info(std::move(lock_info))
    {}

    RegionID region_id;
    LockInfoPtr lock_info;
};

} // namespace DB

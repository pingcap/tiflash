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

#include <Storages/KVStore/Types.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/kvrpcpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
using LockInfoPtr = std::unique_ptr<kvrpcpb::LockInfo>;

// To get lock info from region: read_tso is from cop request, any lock with ts in bypass_lock_ts should be filtered.
struct RegionLockReadQuery
{
    const UInt64 read_tso;
    const std::unordered_set<UInt64> * bypass_lock_ts;
};

} // namespace DB

// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionSerde.h>

namespace DB::RegionBench
{
struct DebugKVStore
{
    DebugKVStore(KVStore & kvs)
        : kvstore(kvs)
    {}
    KVStore * operator->() { return &kvstore; }
    KVStore * operator->() const { return &kvstore; }

    RegionSerdeOpts & mutRegionSerdeOpts();
    template <typename RegionPtrWrap>
    void onSnapshot(const RegionPtrWrap & r, RegionPtr old_region, UInt64 old_region_index, TMTContext & tmt)
    {
        kvstore.onSnapshot<RegionPtrWrap>(r, old_region, old_region_index, tmt);
    }

    void mockRemoveRegion(RegionID region_id, RegionTable & region_table);

private:
    KVStore & kvstore;
};
} // namespace DB::RegionBench
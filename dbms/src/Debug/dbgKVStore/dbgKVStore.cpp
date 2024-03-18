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

#include <Debug/dbgKVStore/dbgKVStore.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>

namespace DB::RegionBench
{

RegionSerdeOpts & DebugKVStore::mutRegionSerdeOpts()
{
    return kvstore.region_persister->region_serde_opts;
}
  
void DebugKVStore::mockRemoveRegion(DB::RegionID region_id, RegionTable & region_table)
{
    auto task_lock = kvstore.genTaskLock();
    auto region_lock = kvstore.region_manager.genRegionTaskLock(region_id);
    // mock remove region should remove data by default
    kvstore.removeRegion(region_id, /* remove_data */ true, region_table, task_lock, region_lock);
}

} // namespace DB::RegionBench
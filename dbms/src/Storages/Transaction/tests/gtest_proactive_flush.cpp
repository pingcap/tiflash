// Copyright 2023 PingCAP, Ltd.
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

#include "kvstore_helper.h"

namespace DB
{
namespace tests
{
TEST_F(RegionKVStoreTest, ProactiveFlush)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    UInt64 region_id = 1;
    TableID table_id;
    KVStore & kvs = getKVS();
    {
        initStorages();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::nullopt);
        auto kvr1 = kvs.getRegion(region_id);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr1);
    }
    {
        auto kvr1 = kvs.getRegion(region_id);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr1);
        auto & r1_range = kvr1->getRange()->comparableKeys();
        LOG_INFO(&Poco::Logger::get("!!!!"), "!!!!! r1 range {} {}", r1_range.first.toDebugString(), r1_range.second.toDebugString());

        auto keyrange = DM::RowKeyRange::newAll(false, 10);
        kvs.compactLogByRowKeyRange(ctx.getTMTContext(), keyrange, DB::NullspaceID, table_id, false);
    }
    {
    }
}
CATCH

} // namespace tests
} // namespace DB
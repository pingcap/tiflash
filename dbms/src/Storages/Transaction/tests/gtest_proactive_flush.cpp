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
TEST_F(RegionKVStoreTest, ProactiveFlushLiveness)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    ctx.getSettingsRef().dt_segment_limit_rows = 1000000;
    ctx.getSettingsRef().dt_segment_limit_size = 1000000;
    ctx.getSettingsRef().dt_segment_delta_cache_limit_rows = 0;
    ctx.getSettingsRef().dt_segment_delta_cache_limit_size = 0;
    UInt64 region_id = 1;
    UInt64 region_id2 = 7;
    TableID table_id;
    KVStore & kvs = getKVS();
    ASSERT_EQ(&ctx.getTMTContext().getContext(), &ctx);
    ASSERT_EQ(ctx.getTMTContext().getContext().getSettingsRef().dt_segment_limit_size, 1000000);
    MockRaftStoreProxy::FailCond cond;
    {
        initStorages();
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        HandleID end_index = 100;
        HandleID mid_index = 50;
        auto start = RecordKVFormat::genKey(table_id, 0);
        auto end = RecordKVFormat::genKey(table_id, end_index);
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), region_id, std::make_pair(start.toString(), end.toString()));

        auto source_region = kvs.getRegion(region_id);
        auto old_epoch = source_region->mutMeta().getMetaRegion().region_epoch();
        auto && [request, response] = MockRaftStoreProxy::composeBatchSplit(
            {region_id, region_id2},
            {{RecordKVFormat::genKey(table_id, mid_index), RecordKVFormat::genKey(table_id, end_index)},
             {RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, mid_index)}},
            old_epoch);
        auto && [index2, term2] = proxy_instance->adminCommand(region_id, std::move(request), std::move(response));
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);

        auto kvr1 = kvs.getRegion(region_id);
        auto kvr2 = kvs.getRegion(region_id2);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr1);
        ctx.getTMTContext().getRegionTable().updateRegion(*kvr2);
    }
    std::shared_ptr<std::atomic<size_t>> ai = std::make_shared<std::atomic<size_t>>();
    {
        // A fg flush and a bg flush will not deadlock.
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_before_persist_region);
        ai->store(0b1011);
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_force_set_type, ai);
        auto f1 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
            // Trigger a forground flush on region_id
            auto [index, term] = proxy_instance->rawWrite(region_id, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2, std::make_optional(true));
        };
        std::thread t1(f1);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ai->store(0b1110);
        // Force bg flush.
        auto f2 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 5, 111);
            // Trigger a forground flush on region_id2
            auto [index, term] = proxy_instance->rawWrite(region_id2, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id2, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2, std::make_optional(false));
        };
        std::thread t2(f2);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_before_persist_region);
        t1.join();
        t2.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
        // We can't assert for region_id2, since bg flush may be be finished.
    }
    kvs.setRegionCompactLogConfig(0, 0, 0); // Every notify will take effect.
    LOG_INFO(&Poco::Logger::get("!!!!"), "!!!!! next");
    {
        // Two fg flush will not deadlock.
        DB::FailPointHelper::enableFailPoint(DB::FailPoints::proactive_flush_before_persist_region);
        ai->store(0b1011);
        auto f1 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 60, 111);
            // Trigger a forground flush on region_id
            auto [index, term] = proxy_instance->rawWrite(region_id, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index2);
        };
        auto f2 = [&]() {
            auto && [value_write, value_default] = proxy_instance->generateTiKVKeyValue(111, 999);
            auto k1 = RecordKVFormat::genKey(table_id, 5, 111);
            // Trigger a forground flush on region_id2
            auto [index, term] = proxy_instance->rawWrite(region_id2, {k1}, {value_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            auto [index2, term2] = proxy_instance->rawWrite(region_id2, {k1}, {value_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index);
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id2, index2);
        };
        std::thread t1(f1);
        std::thread t2(f2);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        DB::FailPointHelper::disableFailPoint(DB::FailPoints::proactive_flush_before_persist_region);
        t1.join();
        t2.join();
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
        ASSERT_EQ(proxy_instance->getRegion(region_id2)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id2)->getLatestCommitIndex());
    }
    {
        // An obsolete notification triggered by another region's flush shall not override.
        kvs.notifyCompactLog(region_id, 1, 5, true, false);
        ASSERT_EQ(proxy_instance->getRegion(region_id)->getApply().truncated_state().index(), proxy_instance->getRegion(region_id)->getLatestCommitIndex());
    }
}
CATCH

TEST_F(RegionKVStoreTest, ProactiveFlushRecover)
try
{
    {
        // Safe to abort between flushing regions.
    } {
        // Safe to abort between flushCache and persistRegion.
    }
}
CATCH

} // namespace tests
} // namespace DB
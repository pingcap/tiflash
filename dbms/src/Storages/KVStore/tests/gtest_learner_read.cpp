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

#include <Interpreters/Context.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/LearnerReadWorker.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <Storages/PathPool.h>
#include <Storages/RegionQueryInfo.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <kvproto/kvrpcpb.pb.h>

namespace DB::tests
{
class LearnerReadTest : public ::testing::Test
{
public:
    LearnerReadTest()
        : log(Logger::get())
    {}

protected:
    static kvrpcpb::ReadIndexResponse makeReadIndexResult(UInt64 index)
    {
        auto resp = kvrpcpb::ReadIndexResponse();
        resp.set_read_index(index);
        return resp;
    }

    // helper function for testing private method
    static std::vector<kvrpcpb::ReadIndexRequest> buildBatchReadIndex( //
        LearnerReadWorker & worker,
        const RegionTable & region_table,
        const LearnerReadSnapshot & snapshot,
        RegionsReadIndexResult & read_index_result)
    {
        return worker.buildBatchReadIndexReq(region_table, snapshot, read_index_result);
    }

    static void recordReadIndexError(
        const LearnerReadSnapshot & regions_snapshot,
        LearnerReadWorker & worker,
        RegionsReadIndexResult & read_index_result)
    {
        worker.recordReadIndexError(regions_snapshot, read_index_result);
    }

protected:
    LoggerPtr log;
};

TEST_F(LearnerReadTest, BuildReadIndexRequests)
try
{
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = global_ctx.getTMTContext();

    const RegionID region_id_200 = 200;
    const RegionID region_id_201 = 201;
    const RegionID region_id_202 = 202;
    const RegionID region_id_203 = 203;

    const TableID table_id = 100;

    LearnerReadSnapshot snapshot{
        {
            region_id_200,
            RegionLearnerReadSnapshot(makeRegion(
                region_id_200,
                RecordKVFormat::genKey(table_id, 0),
                RecordKVFormat::genKey(table_id, 10000))),
        },
        {
            region_id_201,
            RegionLearnerReadSnapshot(makeRegion(
                region_id_201,
                RecordKVFormat::genKey(table_id, 10000),
                RecordKVFormat::genKey(table_id, 20000))),
        },
        {
            region_id_202,
            RegionLearnerReadSnapshot(makeRegion(
                region_id_202,
                RecordKVFormat::genKey(table_id, 20000),
                RecordKVFormat::genKey(table_id, 30000))),
        },
        {
            region_id_203,
            RegionLearnerReadSnapshot(makeRegion(
                region_id_203,
                RecordKVFormat::genKey(table_id, 30000),
                RecordKVFormat::genKey(table_id, 40000))),
        },
    };

    MvccQueryInfo mvcc_query_info(false, 10000, nullptr);
    for (const auto & [region_id, region] : snapshot)
    {
        mvcc_query_info.regions_query_info.emplace_back(RegionQueryInfo{
            region_id,
            region->version(),
            region->confVer(),
            table_id,
        });
    }
    mvcc_query_info.addReadIndexResToCache(region_id_203, 60);
    LearnerReadWorker worker(mvcc_query_info, tmt, true, false, log);

    // region_200 can stale read
    // region_201 can not stale read
    // region_202 has no safe ts info, can not stale read
    // region_203 read index cache exist in the `mvcc_query_info`
    RegionTable region_table(global_ctx);
    region_table.updateSafeTS(region_id_200, /*leader_safe_ts*/ 10005, /*self_safe_ts*/ 10005);
    region_table.updateSafeTS(region_id_201, /*leader_safe_ts*/ 9995, /*self_safe_ts*/ 9995);

    RegionsReadIndexResult read_index_result;
    auto requests = buildBatchReadIndex(worker, region_table, snapshot, read_index_result);
    // check requests
    ASSERT_EQ(requests.size(), 2);

    // check read_index_result
    ASSERT_EQ(read_index_result.size(), 2);
    ASSERT_TRUE(read_index_result.contains(region_id_200)); // stale read
    ASSERT_EQ(read_index_result[region_id_200].read_index(), 0);

    ASSERT_TRUE(read_index_result.contains(region_id_203)); // read index cache
    ASSERT_EQ(read_index_result[region_id_203].read_index(), 60);

    const auto & stats = worker.getStats();
    ASSERT_EQ(stats.num_regions, 4);
    ASSERT_EQ(stats.num_stale_read, 1);
    ASSERT_EQ(stats.num_cached_read_index, 1);
    ASSERT_EQ(stats.num_read_index_request, 2);
    ASSERT_EQ(stats.num_regions, stats.num_stale_read + stats.num_cached_read_index + stats.num_read_index_request);
}
CATCH

TEST_F(LearnerReadTest, CacheReadIndexResult)
try
{
    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = global_ctx.getTMTContext();

    MvccQueryInfo mvcc_query_info(false, 10000, nullptr);
    LearnerReadWorker worker(mvcc_query_info, tmt, true, false, log);

    RegionsReadIndexResult read_index_result{
        {200, makeReadIndexResult(0)}, // stale read, won't be added to cache
        {201, makeReadIndexResult(24)},
        {202, makeReadIndexResult(1024)},
        {203, makeReadIndexResult(10000)},
    };
    LearnerReadSnapshot regions_snapshot = worker.buildRegionsSnapshot();
    recordReadIndexError(regions_snapshot, worker, read_index_result);

    ASSERT_EQ(0, mvcc_query_info.getReadIndexRes(200));
    ASSERT_EQ(24, mvcc_query_info.getReadIndexRes(201));
    ASSERT_EQ(1024, mvcc_query_info.getReadIndexRes(202));
    ASSERT_EQ(10000, mvcc_query_info.getReadIndexRes(203));
    ASSERT_EQ(0, mvcc_query_info.getReadIndexRes(999)); // not exist region_id
}
CATCH

} // namespace DB::tests

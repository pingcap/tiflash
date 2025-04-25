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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/SyncPoint/Ctl.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/tests/gtest_dm_vector_index_utils.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <TestUtils/InputStreamTestUtils.h>

namespace DB::FailPoints
{
extern const char force_local_index_task_memory_limit_exceeded[];
extern const char exception_build_local_index_for_file[];
} // namespace DB::FailPoints

namespace DB::DM::tests
{

class DeltaMergeStoreVectorTest
    : public DB::base::TiFlashStorageTestBasic
    , public VectorIndexTestUtils
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        global_context.getTMTContext().initS3GCManager(nullptr);
        store = reload();
    }

    DeltaMergeStorePtr reload(LocalIndexInfosPtr default_local_index = nullptr)
    {
        TiFlashStorageTestBasic::reload();
        auto cols = DMTestEnv::getDefaultColumns();
        cols->push_back(cdVec());

        ColumnDefine handle_column_define = (*cols)[0];

        if (!default_local_index)
            default_local_index = indexInfo();

        DeltaMergeStorePtr s = DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_100",
            NullspaceID,
            100,
            /*pk_col_id*/ 0,
            true,
            *cols,
            handle_column_define,
            false,
            1,
            default_local_index,
            DeltaMergeStore::Settings());
        return s;
    }

    void write(size_t begin, size_t end, bool is_delete = false, int dimension = 1)
    {
        String sequence = fmt::format("[{}, {})", begin, end);
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(
                begin,
                end,
                false,
                /*tso= */ 3,
                /*pk_name_=*/MutSup::extra_handle_column_name,
                /*pk_col_id=*/MutSup::extra_handle_id,
                /*pk_type=*/MutSup::getExtraHandleColumnIntType(),
                /*is_common_handle=*/false,
                /*rowkey_column_size=*/1,
                /*with_internal_columns=*/true,
                is_delete);
            // Add a column of vector for test
            block.insert(colVecFloat32(sequence, vec_column_name, vec_column_id, dimension));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    /// Only read one vector column.
    void readVec(const RowKeyRange & range, const PushDownExecutorPtr & filter, const ColumnWithTypeAndName & out)
    {
        readColumns(range, filter, {cdVec()}, {out});
    }

    void readColumns(
        const RowKeyRange & range,
        const PushDownExecutorPtr & filter,
        const ColumnDefines col_defs,
        const ColumnsWithTypeAndName & out)
    {
        auto in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            col_defs,
            {range},
            /* num_streams= */ 1,
            /* max_version= */ std::numeric_limits<UInt64>::max(),
            filter,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /*keep_order=*/false)[0];
        Strings cols_name;
        for (const auto & cd : col_defs)
        {
            cols_name.push_back(cd.name);
        }
        ASSERT_INPUTSTREAM_COLS_UR(in, cols_name, out);
    }

    ColumnDefine cdPK() const { return getExtraHandleColumnDefine(store->is_common_handle); }

    void triggerMergeDelta() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        for (const auto & segment : all_segments)
            ASSERT_TRUE(
                store->segmentMergeDelta(*dm_context, segment, DeltaMergeStore::MergeDeltaReason::Manual) != nullptr);
    }

    void triggerFlushCacheAndEnsureDeltaLocalIndex() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        for (const auto & segment : all_segments)
        {
            ASSERT_TRUE(segment->flushCache(*dm_context));
            store->segmentEnsureDeltaLocalIndexAsync(segment);
        }
    }

    void triggerFlushCache() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        for (const auto & segment : all_segments)
        {
            ASSERT_TRUE(segment->flushCache(*dm_context));
        }
    }

    void triggerCompactDelta() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        for (const auto & segment : all_segments)
            ASSERT_TRUE(segment->compactDelta(*dm_context));
    }

    void waitStableLocalIndexReady()
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        for (const auto & segment : all_segments)
            ASSERT_TRUE(store->segmentWaitStableLocalIndexReady(segment));
    }

    void waitDeltaIndexReady()
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        for (const auto & segment : all_segments)
            ASSERT_TRUE(store->segmentWaitDeltaLocalIndexReady(segment));
    }

    void triggerMergeAllSegments()
    {
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        std::vector<SegmentPtr> segments_to_merge;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                segments_to_merge.push_back(segment);
        }
        std::sort(segments_to_merge.begin(), segments_to_merge.end(), [](const auto & lhs, const auto & rhs) {
            return lhs->getRowKeyRange().getEnd() < rhs->getRowKeyRange().getEnd();
        });
        auto new_segment = store->segmentMerge(
            *dm_context,
            segments_to_merge,
            DeltaMergeStore::SegmentMergeReason::BackgroundGCThread);
        ASSERT_TRUE(new_segment != nullptr);
    }

protected:
    DeltaMergeStorePtr store;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreVectorTest";
};

TEST_F(DeltaMergeStoreVectorTest, TestBasic)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);
    // write delete [0, 64) to store
    write(0, num_rows_write / 2, true);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();
    // check stable index has built for all segments
    waitStableLocalIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[64, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {127.5}, .top_k = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{127.0}, {128.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {72.1}, .top_k = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{72.0}, {73.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistance)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);
    // write delete [0, 64) to store
    write(0, num_rows_write / 2, true);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();
    // check stable index has built for all segments
    waitStableLocalIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {127.5},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createNullableColumn<Float32>({0.5, 0.5}, {0, 0})});
    }

    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {130},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createNullableColumn<Float32>({0, 1}, {0, 0})});
    }

    // Read nullable vector
    // Note: write() has insert not nullable vector data. This case just for testing nullable assertion in the initialization of
    // DistanceProjectionInputStream
    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {130},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
            .vector_is_nullable = true, // set true for assertion
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createNullableColumn<Float32>({0, 1}, {0, 0})});
    }

    // read (id, dis) columns.
    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {130},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {cdPK(), VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createColumn<Int64>({130, 131}), createNullableColumn<Float32>({0, 1}, {0, 0})});
    }

    // enable_distance_proj is false
    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {127.5},
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        try
        {
            readColumns(
                range,
                filter,
                {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
                {createNullableColumn<Float32>({0.5, 0.5}, {0, 0})});
            FAIL();
        }
        catch (const DB::Exception & ex)
        {
            EXPECT_TRUE(
                ex.message().find("Check cd.id != VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD.id failed: got distance "
                                  "column but enable_distance_proj is false, please check the creation of plan.")
                != std::string::npos)
                << ex.message();
        }
        catch (...)
        {
            FAIL();
        }
    }

    // read a not nullable distance column
    {
        auto ann_query_info = annQueryInfoTopK({
            .vec = {127.5},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        try
        {
            readColumns(
                range,
                filter,
                {ColumnDefine(
                    -2000,
                    "_INTERNAL_VEC_SEARCH_DISTANCE",
                    DataTypeFactory::instance().get("Float32"))}, // not nullable
                {createNullableColumn<Float32>({0.5, 0.5}, {0, 0})});
            FAIL();
        }
        catch (const DB::Exception & ex)
        {
            EXPECT_TRUE(
                ex.message().find(
                    "Check col_defs->back().type->isNullable() failed: the distance column is expected as "
                    "Nullable(Float32) but got Float32, please check the creation of distance column in TiDB.")
                != std::string::npos)
                << ex.message();
        }
        catch (...)
        {
            FAIL();
        }
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceMemtable)
try
{
    store = reload();
    write(0, 4);

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({64, 63, 62, 61}, {0, 0, 0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceTinyNoIndex)
try
{
    store = reload();
    write(0, 4);
    triggerFlushCache();

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({64, 63, 62, 61}, {0, 0, 0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceTinyWithIndex)
try
{
    store = reload();
    write(0, 128);
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    waitDeltaIndexReady();

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({0.0, 1.0}, {0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceTinyWithIndexAndMemtable)
try
{
    store = reload();
    write(0, 128);
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    waitDeltaIndexReady();
    write(128, 132);

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({0, 1, /* from memtable */ 64, 65, 66, 67}, {0, 0, 0, 0, 0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceStableNoIndex)
try
{
    store = reload();
    write(0, 4);
    triggerMergeDelta();

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({64, 63, 62, 61}, {0, 0, 0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceStableWithIndex)
try
{
    store = reload();
    write(0, 128);
    triggerMergeDelta();
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    waitStableLocalIndexReady();

    auto ann_query_info = annQueryInfoTopK({
        .vec = {64},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });


    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>({0, 1}, {0, 0})});
}
CATCH

/// Test read distance when stable(index) + tiny(index) + tiny(no index) + memtable
TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceHybrid)
try
{
    store = reload();
    // Stable with index
    write(0, 64);
    triggerMergeDelta();
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    waitStableLocalIndexReady();

    // Tiny with index
    write(64, 192);
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    waitDeltaIndexReady();

    // Tiny without index
    write(192, 196);
    triggerFlushCache();

    // Memtable
    write(196, 200);

    auto ann_query_info = annQueryInfoTopK({
        .vec = {150},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
    readColumns(
        RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size),
        filter,
        {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
        {createNullableColumn<Float32>(
            {
                0,
                1, // from index (2 result)
                42,
                43,
                44,
                45, // from tiny (4 result)
                46,
                47,
                48,
                49 // from memtable (4 result)
            },
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0})});
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceIfNotLastPosition)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);
    // write delete [0, 64) to store
    write(0, num_rows_write / 2, true);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();
    // check stable index has built for all segments
    waitStableLocalIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    auto ann_query_info = annQueryInfoTopK({
        .vec = {150},
        .enable_distance_proj = true,
        .top_k = 2,
        .column_id = vec_column_id,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });

    auto filter = std::make_shared<PushDownExecutor>(ann_query_info);

    try
    {
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD, cdPK()},
            {createColumn<Int64>({130, 131}), createNullableColumn<Float32>({0, 1}, {0, 0})});
        FAIL();
    }
    catch (const DB::Exception & ex)
    {
        EXPECT_TRUE(
            ex.message().find("Check cd.id != VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD.id failed: got more than one "
                              "distance column, please check the creation of plan.")
            != std::string::npos)
            << ex.message();
    }
    catch (...)
    {
        FAIL();
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestReadDistanceIfSqrt)
try
{
    // read with L2 distance
    {
        store = reload();

        const size_t num_rows_write = 128;

        // write [0, 128) to store
        write(0, num_rows_write);
        // trigger mergeDelta for all segments
        triggerMergeDelta();

        // write [128, 256) to store
        write(num_rows_write, num_rows_write * 2);
        // write delete [0, 64) to store
        write(0, num_rows_write / 2, true);

        // trigger FlushCache for all segments
        triggerFlushCacheAndEnsureDeltaLocalIndex();

        // check delta index has built for all segments
        waitDeltaIndexReady();
        // check stable index has built for all segments
        waitStableLocalIndexReady();

        const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

        auto ann_query_info = annQueryInfoTopK({
            //
            .vec = {127.5},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createNullableColumn<Float32>({0.5, 0.5}, {0, 0})});
    }

    // read with cosine distance
    {
        const LocalIndexInfos index_infos = LocalIndexInfos{
            LocalIndexInfo{
                EmptyIndexID,
                vec_column_id,
                std::make_shared<TiDB::VectorIndexDefinition>(
                    tipb::VectorIndexKind::HNSW,
                    1,
                    tipb::VectorDistanceMetric::COSINE)},
        };

        store = reload(std::make_shared<LocalIndexInfos>(index_infos));

        const size_t num_rows_write = 128;

        // write [0, 128) to store
        write(0, num_rows_write);
        // trigger mergeDelta for all segments
        triggerMergeDelta();

        // write [128, 256) to store
        write(num_rows_write, num_rows_write * 2);
        // write delete [0, 64) to store
        write(0, num_rows_write / 2, true);

        // trigger FlushCache for all segments
        triggerFlushCacheAndEnsureDeltaLocalIndex();

        // check delta index has built for all segments
        waitDeltaIndexReady();
        // check stable index has built for all segments
        waitStableLocalIndexReady();

        const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

        auto ann_query_info = annQueryInfoTopK({
            //
            .vec = {127.5},
            .enable_distance_proj = true,
            .top_k = 2,
            .column_id = vec_column_id,
            .distance_metric = tipb::VectorDistanceMetric::COSINE,
        });

        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readColumns(
            range,
            filter,
            {VectorIndexStreamCtx::VIRTUAL_DISTANCE_CD},
            {createNullableColumn<Float32>({0.0, 0.0}, {0, 0})});
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestMultipleColumnFileTiny)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {72.1}, .top_k = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{72.0}, {73.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {127.5}, .top_k = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{127.0}, {128.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestFlushCache)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    auto sp_delta_index_built
        = SyncPointCtl::enableInScope("DeltaMergeStore::segmentEnsureDeltaLocalIndex_after_build");
    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // Pause after delta vector index built but not set.
    sp_delta_index_built.waitAndPause();

    // write [128, 130) to store
    write(num_rows_write, num_rows_write + 2);
    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // Now persisted file set has changed.
    // Resume
    sp_delta_index_built.next();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 130)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {72.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        // [0, 128) with vector index return 72.0, [128, 130) without vector index return all.
        readVec(range, filter, createVecFloat32Column<Array>({{72.0}, {128.0}, {129.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {72.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        // [0, 128) with vector index return 72.0, [128, 130) without vector index return all.
        readVec(range, filter, createVecFloat32Column<Array>({{72.0}, {128.0}, {129.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestCompactDelta)
try
{
    store = reload();

    auto sp_delta_index_built
        = SyncPointCtl::enableInScope("DeltaMergeStore::segmentEnsureDeltaLocalIndex_after_build");
    // write [0, 2) to store
    write(0, 2);
    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();
    // write [2, 4) to store
    write(2, 4);
    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // Pause after delta vector index built but not set.
    sp_delta_index_built.waitAndPause();

    // compact delta [0, 2) + [2, 4) -> [0, 4)
    triggerCompactDelta();

    // Now persisted file set has changed.
    // Resume
    sp_delta_index_built.next();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 4)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {1.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        // [0, 4) without vector index return all.
        readVec(range, filter, createVecFloat32Column<Array>({{0.0}, {1.0}, {2.0}, {3.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {1.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        // [0, 4) without vector index return all.
        readVec(range, filter, createVecFloat32Column<Array>({{0.0}, {1.0}, {2.0}, {3.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestLogicalSplitAndMerge)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();

    // logical split
    RowKeyRange left_segment_range;
    {
        SegmentPtr segment;
        {
            std::shared_lock lock(store->read_write_mutex);
            segment = store->segments.begin()->second;
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto breakpoint = RowKeyValue::fromHandle(num_rows_write);
        const auto [left, right] = store->segmentSplit(
            *dm_context,
            segment,
            DeltaMergeStore::SegmentSplitReason::ForIngest,
            breakpoint,
            DeltaMergeStore::SegmentSplitMode::Logical);
        ASSERT_TRUE(left->rowkey_range.end == breakpoint);
        ASSERT_TRUE(right->rowkey_range.start == breakpoint);
        left_segment_range = RowKeyRange(
            left->rowkey_range.start,
            left->rowkey_range.end,
            store->is_common_handle,
            store->rowkey_column_size);
    }

    // check stable index has built for all segments
    waitStableLocalIndexReady();

    // read from store
    {
        readVec(
            left_segment_range,
            EMPTY_FILTER,
            colVecFloat32(fmt::format("[0, {})", num_rows_write), vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(left_segment_range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {222.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(left_segment_range, filter, createVecFloat32Column<Array>({{127.0}}));
    }

    // merge segment
    triggerMergeAllSegments();

    // check stable index has built for all segments
    waitStableLocalIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {122.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{122.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestPhysicalSplitAndMerge)
try
{
    // Physical split is slow, so if we trigger mergeDelta and then physical split soon,
    // the physical split is likely to fail since vector index building cause segment to be invalid.

    store = reload();

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();

    // physical split
    auto physical_split = [&] {
        SegmentPtr segment;
        {
            std::shared_lock lock(store->read_write_mutex);
            segment = store->segments.begin()->second;
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto breakpoint = RowKeyValue::fromHandle(num_rows_write);
        return store->segmentSplit(
            *dm_context,
            segment,
            DeltaMergeStore::SegmentSplitReason::ForIngest,
            breakpoint,
            DeltaMergeStore::SegmentSplitMode::Physical);
    };

    auto [left, right] = physical_split();
    if (left == nullptr && right == nullptr)
    {
        // check stable index has built for all segments first
        waitStableLocalIndexReady();
        // trigger physical split again
        std::tie(left, right) = physical_split();
    }

    ASSERT_TRUE(left->rowkey_range.end == RowKeyValue::fromHandle(num_rows_write));
    ASSERT_TRUE(right->rowkey_range.start == RowKeyValue::fromHandle(num_rows_write));
    RowKeyRange left_segment_range = RowKeyRange(
        left->rowkey_range.start,
        left->rowkey_range.end,
        store->is_common_handle,
        store->rowkey_column_size);

    // check stable index has built for all segments
    waitStableLocalIndexReady();

    // read from store
    {
        readVec(
            left_segment_range,
            EMPTY_FILTER,
            colVecFloat32(fmt::format("[0, {})", num_rows_write), vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(left_segment_range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {222.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(left_segment_range, filter, createVecFloat32Column<Array>({{127.0}}));
    }

    // merge segment
    triggerMergeAllSegments();

    // check stable index has built for all segments
    waitStableLocalIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {122.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{122.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestIngestData)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write to store
    write(0, num_rows_write);

    // Prepare DMFile
    auto [dmfile_parent_path, file_id] = store->preAllocateIngestFile();
    ASSERT_FALSE(dmfile_parent_path.empty());
    DMFilePtr dmfile = DMFile::create(
        file_id,
        dmfile_parent_path,
        std::make_optional<DMChecksumConfig>(),
        128 * 1024,
        16 * 1024 * 1024,
        DMFileFormat::V3);
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // Add a column of vector for test
        block.insert(colVecFloat32(fmt::format("[0, {})", num_rows_write), vec_column_name, vec_column_id));
        ColumnDefinesPtr cols = DMTestEnv::getDefaultColumns();
        cols->push_back(cdVec());
        auto stream = std::make_shared<DMFileBlockOutputStream>(*db_context, dmfile, *cols);
        stream->writePrefix();
        stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }
    auto page_id = dmfile->pageId();
    auto file_provider = db_context->getFileProvider();
    dmfile = DMFile::restore(
        file_provider,
        file_id,
        page_id,
        dmfile_parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 0);
    auto delegator = store->path_pool->getStableDiskDelegator();
    delegator.addDTFile(file_id, dmfile->getBytesOnDisk(), dmfile_parent_path);

    // Ingest data
    {
        // Ingest data into the first segment
        auto segment = store->segments.begin()->second;
        auto range = segment->getRowKeyRange();

        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto new_segment = store->segmentIngestData(*dm_context, segment, dmfile, true);
        ASSERT_TRUE(new_segment != nullptr);
    }

    // check stable index has built for all segments
    waitStableLocalIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }
}
CATCH


TEST_F(DeltaMergeStoreVectorTest, TestStoreRestore)
try
{
    store = reload();
    {
        auto local_index_snap = store->getLocalIndexInfosSnapshot();
        ASSERT_NE(local_index_snap, nullptr);
        ASSERT_EQ(local_index_snap->size(), 1);
        const auto & index = (*local_index_snap)[0];
        ASSERT_EQ(index.kind, TiDB::ColumnarIndexKind::Vector);
        ASSERT_EQ(index.index_id, EmptyIndexID);
        ASSERT_EQ(index.column_id, vec_column_id);
        ASSERT_EQ(index.def_vector_index->kind, tipb::VectorIndexKind::HNSW);
        ASSERT_EQ(index.def_vector_index->dimension, 1);
        ASSERT_EQ(index.def_vector_index->distance_metric, tipb::VectorDistanceMetric::L2);
    }

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);

    // trigger FlushCache for all segments
    triggerFlushCacheAndEnsureDeltaLocalIndex();

    // check delta index has built for all segments
    waitDeltaIndexReady();

    // shutdown store
    store->shutdown();

    // restore store
    store = reload();

    // check stable index has built for all segments
    waitStableLocalIndexReady();
    {
        auto local_index_snap = store->getLocalIndexInfosSnapshot();
        ASSERT_NE(local_index_snap, nullptr);
        ASSERT_EQ(local_index_snap->size(), 1);
        const auto & index = (*local_index_snap)[0];
        ASSERT_EQ(index.kind, TiDB::ColumnarIndexKind::Vector);
        ASSERT_EQ(index.index_id, EmptyIndexID);
        ASSERT_EQ(index.column_id, vec_column_id);
        ASSERT_EQ(index.def_vector_index->kind, tipb::VectorIndexKind::HNSW);
        ASSERT_EQ(index.def_vector_index->dimension, 1);
        ASSERT_EQ(index.def_vector_index->distance_metric, tipb::VectorDistanceMetric::L2);
    }

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {222.1}, .top_k = 1});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
        readVec(range, filter, createVecFloat32Column<Array>({{222.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, DDLAddVectorIndex)
try
{
    {
        auto indexes = std::make_shared<LocalIndexInfos>();
        store = reload(indexes);
        ASSERT_EQ(store->getLocalIndexInfosSnapshot(), nullptr);
    }

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2);

    // trigger FlushCache for all segments
    triggerFlushCache();

    {
        // Add vecotr index
        TiDB::TableInfo new_table_info_with_vector_index;
        TiDB::ColumnInfo column_info;
        column_info.name = VectorIndexTestUtils::vec_column_name;
        column_info.id = VectorIndexTestUtils::vec_column_id;
        new_table_info_with_vector_index.columns.emplace_back(column_info);
        TiDB::IndexInfo index;
        index.id = 2;
        TiDB::IndexColumnInfo index_col_info;
        index_col_info.name = VectorIndexTestUtils::vec_column_name;
        index_col_info.offset = 0;
        index.idx_cols.emplace_back(index_col_info);
        index.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });
        new_table_info_with_vector_index.index_infos.emplace_back(index);
        // apply local index change, shuold
        // - create the local index
        // - generate the background tasks for building index on stable
        store->applyLocalIndexChange(new_table_info_with_vector_index);
        ASSERT_EQ(store->local_index_infos->size(), 1);
    }

    // check delta index has built for all segments
    waitDeltaIndexReady();
    // check stable index has built for all segments
    waitStableLocalIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {2.0}, .top_k = 1, .index_id = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);

        readVec(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        const auto ann_query_info = annQueryInfoTopK({.vec = {222.1}, .top_k = 1, .index_id = 2});
        auto filter = std::make_shared<PushDownExecutor>(ann_query_info);

        readVec(range, filter, createVecFloat32Column<Array>({{222.0}}));
    }

    {
        // vector index is dropped
        TiDB::TableInfo new_table_info_with_vector_index;
        TiDB::ColumnInfo column_info;
        column_info.name = VectorIndexTestUtils::vec_column_name;
        column_info.id = VectorIndexTestUtils::vec_column_id;
        new_table_info_with_vector_index.columns.emplace_back(column_info);
        // apply local index change, shuold drop the local index
        store->applyLocalIndexChange(new_table_info_with_vector_index);
        ASSERT_EQ(store->local_index_infos->size(), 0);
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, DDLAddMultipleVectorIndex)
try
{
    {
        auto indexes = std::make_shared<LocalIndexInfos>();
        store = reload(indexes);
        ASSERT_EQ(store->getLocalIndexInfosSnapshot(), nullptr);
    }

    const size_t num_rows_write = 128;

    // write [0, 128) to store
    write(0, num_rows_write, false, 2);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // write [128, 256) to store
    write(num_rows_write, num_rows_write * 2, false, 2);

    // trigger FlushCache for all segments
    triggerFlushCache();

    auto add_vector_index
        = [&](std::vector<IndexID> index_id, std::vector<tipb::VectorDistanceMetric> metrics, UInt64 dimension) {
              TiDB::TableInfo new_table_info_with_vector_index;
              TiDB::ColumnInfo column_info;
              column_info.name = VectorIndexTestUtils::vec_column_name;
              column_info.id = VectorIndexTestUtils::vec_column_id;
              new_table_info_with_vector_index.columns.emplace_back(column_info);
              TiDB::IndexColumnInfo index_col_info;
              index_col_info.name = VectorIndexTestUtils::vec_column_name;
              index_col_info.offset = 0;
              for (size_t i = 0; i < index_id.size(); ++i)
              {
                  TiDB::IndexInfo index;
                  index.id = index_id[i];
                  index.idx_cols.push_back(index_col_info);
                  index.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
                      .kind = tipb::VectorIndexKind::HNSW,
                      .dimension = dimension,
                      .distance_metric = metrics[i],
                  });
                  new_table_info_with_vector_index.index_infos.emplace_back(index);
              }
              // apply local index change, should
              // - create the local index
              // - generate the background tasks for building index on stable and delta
              store->applyLocalIndexChange(new_table_info_with_vector_index);
              ASSERT_EQ(store->local_index_infos->size(), index_id.size());

              // check delta index has built for all segments
              waitDeltaIndexReady();
              // check stable index has built for all segments
              waitStableLocalIndexReady();
          };

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);
    auto query = [&](IndexID index_id,
                     tipb::VectorDistanceMetric metric,
                     const InferredDataVector<Array> & result_1,
                     const InferredDataVector<Array> & result_2) {
        // read from store
        {
            readVec(range, EMPTY_FILTER, colVecFloat32("[0, 256)", vec_column_name, vec_column_id, 2));
        }

        // read with ANN query
        {
            const auto ann_query_info = annQueryInfoTopK({
                .vec = {12.0, 42.0},
                .top_k = 1,
                .index_id = index_id,
                .distance_metric = metric,
            });
            auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
            readVec(range, filter, createVecFloat32Column<Array>(result_1));
        }

        // read with ANN query
        {
            const auto ann_query_info = annQueryInfoTopK({
                .vec = {106.5, 62.3},
                .top_k = 1,
                .index_id = index_id,
                .distance_metric = metric,
            });
            auto filter = std::make_shared<PushDownExecutor>(ann_query_info);
            readVec(range, filter, createVecFloat32Column<Array>(result_2));
        }
    };

    // Add COSINE vector index
    add_vector_index({1}, {tipb::VectorDistanceMetric::COSINE}, 2);
    query(1, tipb::VectorDistanceMetric::COSINE, {{103.0, 103.0}}, {{101.0, 101.0}});

    // Add L2 vector index
    add_vector_index({1, 2}, {tipb::VectorDistanceMetric::COSINE, tipb::VectorDistanceMetric::L2}, 2);
    query(1, tipb::VectorDistanceMetric::COSINE, {{103.0, 103.0}}, {{101.0, 101.0}});
    query(2, tipb::VectorDistanceMetric::L2, {{27.0, 27.0}}, {{84.0, 84.0}});

    {
        // vector index is dropped
        TiDB::TableInfo new_table_info_with_vector_index;
        TiDB::ColumnInfo column_info;
        column_info.name = VectorIndexTestUtils::vec_column_name;
        column_info.id = VectorIndexTestUtils::vec_column_id;
        new_table_info_with_vector_index.columns.emplace_back(column_info);
        // apply local index change, shuold drop the local index
        store->applyLocalIndexChange(new_table_info_with_vector_index);
        ASSERT_EQ(store->local_index_infos->size(), 0);
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, DDLAddVectorIndexErrorMemoryExceed)
try
{
    {
        auto indexes = std::make_shared<LocalIndexInfos>();
        store = reload(indexes);
        ASSERT_EQ(store->getLocalIndexInfosSnapshot(), nullptr);
    }

    const size_t num_rows_write = 128;

    // write to store before index built
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    IndexID index_id = 2;
    // Add vecotr index
    TiDB::TableInfo new_table_info_with_vector_index;
    TiDB::ColumnInfo column_info;
    column_info.name = VectorIndexTestUtils::vec_column_name;
    column_info.id = VectorIndexTestUtils::vec_column_id;
    new_table_info_with_vector_index.columns.emplace_back(column_info);
    TiDB::IndexInfo index;
    index.id = index_id;
    TiDB::IndexColumnInfo index_col_info;
    index_col_info.name = VectorIndexTestUtils::vec_column_name;
    index_col_info.offset = 0;
    index.idx_cols.emplace_back(index_col_info);
    index.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 1,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    new_table_info_with_vector_index.index_infos.emplace_back(index);

    // enable failpoint to mock fail to build index due to memory limit
    FailPointHelper::enableFailPoint(FailPoints::force_local_index_task_memory_limit_exceeded);
    store->applyLocalIndexChange(new_table_info_with_vector_index);
    ASSERT_EQ(store->local_index_infos->size(), 1);

    {
        auto indexes_stat = store->getLocalIndexStats();
        ASSERT_EQ(indexes_stat.size(), 1);
        auto index_stat = indexes_stat[0];
        ASSERT_EQ(index_id, index_stat.index_id);
        ASSERT_EQ(VectorIndexTestUtils::vec_column_id, index_stat.column_id);
        ASSERT_FALSE(index_stat.error_message.empty()) << index_stat.error_message;
        ASSERT_NE(index_stat.error_message.find("exceeds limit"), std::string::npos) << index_stat.error_message;
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, DDLAddVectorIndexErrorBuildException)
try
{
    {
        auto indexes = std::make_shared<LocalIndexInfos>();
        store = reload(indexes);
        ASSERT_EQ(store->getLocalIndexInfosSnapshot(), nullptr);
    }

    const size_t num_rows_write = 128;

    // write to store before index built
    write(0, num_rows_write);
    // trigger mergeDelta for all segments
    triggerMergeDelta();

    IndexID index_id = 2;
    // Add vecotr index
    TiDB::TableInfo new_table_info_with_vector_index;
    TiDB::ColumnInfo column_info;
    column_info.name = VectorIndexTestUtils::vec_column_name;
    column_info.id = VectorIndexTestUtils::vec_column_id;
    new_table_info_with_vector_index.columns.emplace_back(column_info);
    TiDB::IndexInfo index;
    index.id = index_id;
    TiDB::IndexColumnInfo index_col_info;
    index_col_info.name = VectorIndexTestUtils::vec_column_name;
    index_col_info.offset = 0;
    index.idx_cols.emplace_back(index_col_info);
    index.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 1,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    new_table_info_with_vector_index.index_infos.emplace_back(index);

    // enable failpoint to mock fail to build index due to memory limit
    FailPointHelper::enableFailPoint(FailPoints::exception_build_local_index_for_file);
    store->applyLocalIndexChange(new_table_info_with_vector_index);
    ASSERT_EQ(store->local_index_infos->size(), 1);

    auto scheduler = db_context->getGlobalLocalIndexerScheduler();
    scheduler->waitForFinish();

    {
        auto indexes_stat = store->getLocalIndexStats();
        ASSERT_EQ(indexes_stat.size(), 1);
        auto index_stat = indexes_stat[0];
        ASSERT_EQ(index_id, index_stat.index_id);
        ASSERT_EQ(VectorIndexTestUtils::vec_column_id, index_stat.column_id);
        ASSERT_FALSE(index_stat.error_message.empty()) << index_stat.error_message;
        ASSERT_NE(index_stat.error_message.find("Fail point"), std::string::npos) << index_stat.error_message;
    }
}
CATCH

} // namespace DB::DM::tests

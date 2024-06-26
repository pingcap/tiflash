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
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_dm_vector_index_utils.h>
#include <TestUtils/InputStreamTestUtils.h>


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
        store = reload();
    }

    DeltaMergeStorePtr reload()
    {
        TiFlashStorageTestBasic::reload();
        auto cols = DMTestEnv::getDefaultColumns();
        cols->push_back(cdVec());

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_100",
            NullspaceID,
            100,
            true,
            *cols,
            handle_column_define,
            false,
            1,
            indexInfo(),
            DeltaMergeStore::Settings());
        return s;
    }

    void write(size_t num_rows_write)
    {
        String sequence = fmt::format("[0, {})", num_rows_write);
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of vector for test
            block.insert(colVecFloat32(sequence, vec_column_name, vec_column_id));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    void read(const RowKeyRange & range, const PushDownFilterPtr & filter, const ColumnWithTypeAndName & out)
    {
        auto in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            {cdVec()},
            {range},
            /* num_streams= */ 1,
            /* max_version= */ std::numeric_limits<UInt64>::max(),
            filter,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /*keep_order=*/false)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({vec_column_name}),
            createColumns({
                out,
            }));
    }

    void triggerMergeDelta()
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

    void waitStableIndexReady()
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        for (const auto & segment : all_segments)
            ASSERT_TRUE(store->segmentWaitStableIndexReady(segment));
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

    // write to store
    write(num_rows_write);

    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // check stable index has built for all segments
    waitStableIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        read(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
    ann_query_info->set_column_id(vec_column_id);
    ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestLogicalSplitAndMerge)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write to store
    write(num_rows_write);

    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // logical split
    RowKeyRange left_segment_range;
    {
        SegmentPtr segment;
        {
            std::shared_lock lock(store->read_write_mutex);
            segment = store->segments.begin()->second;
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto breakpoint = RowKeyValue::fromHandle(num_rows_write / 2);
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
    waitStableIndexReady();

    // read from store
    {
        read(
            left_segment_range,
            EMPTY_FILTER,
            colVecFloat32(fmt::format("[0, {})", num_rows_write / 2), vec_column_name, vec_column_id));
    }

    auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
    ann_query_info->set_column_id(vec_column_id);
    ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(left_segment_range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({122.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(left_segment_range, filter, createVecFloat32Column<Array>({})); // FIXME: should be 63.0
    }

    // merge segment
    triggerMergeAllSegments();

    // check stable index has built for all segments
    waitStableIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        read(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({122.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{122.0}}));
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

    // write to store
    write(num_rows_write);

    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // physical split
    auto physical_split = [&] {
        SegmentPtr segment;
        {
            std::shared_lock lock(store->read_write_mutex);
            segment = store->segments.begin()->second;
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto breakpoint = RowKeyValue::fromHandle(num_rows_write / 2);
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
        waitStableIndexReady();
        // trigger physical split again
        std::tie(left, right) = physical_split();
    }

    ASSERT_TRUE(left->rowkey_range.end == RowKeyValue::fromHandle(num_rows_write / 2));
    ASSERT_TRUE(right->rowkey_range.start == RowKeyValue::fromHandle(num_rows_write / 2));
    RowKeyRange left_segment_range = RowKeyRange(
        left->rowkey_range.start,
        left->rowkey_range.end,
        store->is_common_handle,
        store->rowkey_column_size);

    // check stable index has built for all segments
    waitStableIndexReady();

    // read from store
    {
        read(
            left_segment_range,
            EMPTY_FILTER,
            colVecFloat32(fmt::format("[0, {})", num_rows_write / 2), vec_column_name, vec_column_id));
    }

    auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
    ann_query_info->set_column_id(vec_column_id);
    ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(left_segment_range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({122.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(left_segment_range, filter, createVecFloat32Column<Array>({{63.0}}));
    }

    // merge segment
    triggerMergeAllSegments();

    // check stable index has built for all segments
    waitStableIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        read(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({122.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{122.0}}));
    }
}
CATCH

TEST_F(DeltaMergeStoreVectorTest, TestIngestData)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write to store
    write(num_rows_write);

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
    waitStableIndexReady();

    auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        read(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
    ann_query_info->set_column_id(vec_column_id);
    ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }
}
CATCH


TEST_F(DeltaMergeStoreVectorTest, TestStoreRestore)
try
{
    store = reload();

    const size_t num_rows_write = 128;

    // write to store
    write(num_rows_write);

    // trigger mergeDelta for all segments
    triggerMergeDelta();

    // shutdown store
    store->shutdown();

    // restore store
    store = reload();

    // check stable index has built for all segments
    waitStableIndexReady();

    const auto range = RowKeyRange::newAll(store->is_common_handle, store->rowkey_column_size);

    // read from store
    {
        read(range, EMPTY_FILTER, colVecFloat32("[0, 128)", vec_column_name, vec_column_id));
    }

    auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
    ann_query_info->set_column_id(vec_column_id);
    ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.0}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }

    // read with ANN query
    {
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({2.1}));

        auto filter = std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info));

        read(range, filter, createVecFloat32Column<Array>({{2.0}}));
    }
}
CATCH

} // namespace DB::DM::tests

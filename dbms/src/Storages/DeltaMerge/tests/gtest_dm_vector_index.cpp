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

#include <Common/SyncPoint/Ctl.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/PathPool.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

#include <ext/scope_guard.h>
#include <filesystem>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace DB::FailPoints
{
extern const char force_use_dmfile_format_v3[];
extern const char file_cache_fg_download_fail[];
} // namespace DB::FailPoints

namespace DB::DM::tests
{

class VectorIndexTestUtils
{
public:
    const ColumnID vec_column_id = 100;
    const String vec_column_name = "vec";

    /// Create a column with values like [1], [2], [3], ...
    /// Each value is a VectorFloat32 with exactly one dimension.
    static ColumnWithTypeAndName colInt64(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        return createColumn<Int64>(data, name, column_id);
    }

    static ColumnWithTypeAndName colVecFloat32(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        std::vector<Array> data_in_array;
        for (auto & v : data)
        {
            Array vec;
            vec.push_back(static_cast<Float64>(v));
            data_in_array.push_back(vec);
        }
        return createVecFloat32Column<Array>(data_in_array, name, column_id);
    }

    static String encodeVectorFloat32(const std::vector<Float32> & vec)
    {
        WriteBufferFromOwnString wb;
        Array arr;
        for (const auto & v : vec)
            arr.push_back(static_cast<Float64>(v));
        EncodeVectorFloat32(arr, wb);
        return wb.str();
    }

    ColumnDefine cdVec()
    {
        // When used in read, no need to assign vector_index.
        return ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    }
};

class VectorIndexDMFileTest
    : public VectorIndexTestUtils
    , public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<bool>
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_shared<StoragePathPool>(
            db_context->getPathPool().withTable("test", "VectorIndexDMFileTest", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");
        dm_file = DMFile::create(
            1,
            parent_path,
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            DMFileFormat::V3);

        DB::tests::TiFlashTestEnv::disableS3Config();

        reload();
    }

    // Update dm_context.
    void reload()
    {
        TiFlashStorageTestBasic::reload();

        *path_pool = db_context->getPathPool().withTable("test", "t1", false);
        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMFilePtr restoreDMFile()
    {
        auto file_id = dm_file->fileId();
        auto page_id = dm_file->pageId();
        auto file_provider = dbContext().getFileProvider();
        return DMFile::restore(file_provider, file_id, page_id, parent_path, DMFileMeta::ReadMode::all());
    }

    Context & dbContext() { return *db_context; }

protected:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;

protected:
    String parent_path;
    DMFilePtr dm_file = nullptr;

public:
    VectorIndexDMFileTest() { test_only_vec_column = GetParam(); }

protected:
    // DMFile has different logic when there is only vec column.
    // So we test it independently.
    bool test_only_vec_column = false;

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns) const
    {
        if (!test_only_vec_column)
            return columns;

        // In test_only_vec_column mode, only contains the Array column.
        for (const auto & col : columns)
        {
            if (col.type->getName() == "Array(Float32)")
                return {col};
        }

        RUNTIME_CHECK(false);
    }

    Strings createColumnNames()
    {
        if (!test_only_vec_column)
            return {DMTestEnv::pk_name, vec_column_name};

        // In test_only_vec_column mode, only contains the Array column.
        return {vec_column_name};
    }
};

INSTANTIATE_TEST_CASE_P(VectorIndex, VectorIndexDMFileTest, testing::Bool());

TEST_P(VectorIndexDMFileTest, OnePack)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    cols->emplace_back(vec_cd);

    ColumnDefines read_cols = *cols;
    if (test_only_vec_column)
        read_cols = {vec_cd};

    // Prepare DMFile
    {
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
        block.insert(
            createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}}, vec_cd.name, vec_cd.id));
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();

    // Read with exact match
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.5}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.5}}),
            }));
    }

    // Read with approximate match
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.5}}),
            }));
    }

    // Read multiple rows
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(2);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {1.0, 2.0, 3.5}}),
            }));
    }

    // Read with MVCC filter
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        auto bitmap_filter = std::make_shared<BitmapFilter>(3, true);
        bitmap_filter->set(/* start */ 2, /* limit */ 1, false);

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(bitmap_filter, 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}}),
            }));
    }

    // Query Top K = 0: the pack should be filtered out
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(0);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({}),
                createVecFloat32Column<Array>({}),
            }));
    }

    // Query Top K > rows
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(10);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 1, 2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}}),
            }));
    }

    // Illegal ANNQueryInfo: Ref Vector'dimension is different
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(10);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());

        try
        {
            stream->readPrefix();
            stream->read();
            FAIL();
        }
        catch (const DB::Exception & ex)
        {
            ASSERT_STREQ("Query vector size 1 does not match index dimensions 3", ex.message().c_str());
        }
        catch (...)
        {
            FAIL();
        }
    }

    // Illegal ANNQueryInfo: Referencing a non-existed column. This simply cause vector index not used.
    // The query will not fail, because ANNQueryInfo is passed globally in the whole read path.
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(5);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 1, 2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}}),
            }));
    }

    // Illegal ANNQueryInfo: Different distance metric.
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::COSINE);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());

        try
        {
            stream->readPrefix();
            stream->read();
            FAIL();
        }
        catch (const DB::Exception & ex)
        {
            ASSERT_STREQ("Query distance metric COSINE does not match index distance metric L2", ex.message().c_str());
        }
        catch (...)
        {
            FAIL();
        }
    }

    // Illegal ANNQueryInfo: The column exists but is not a vector column.
    // Currently the query is fine and ANNQueryInfo is discarded, because we discovered that this column
    // does not have index at all.
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(EXTRA_HANDLE_COLUMN_ID);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.8}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(3, true), 0, 3))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 1, 2}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}}),
            }));
    }
}
CATCH

TEST_P(VectorIndexDMFileTest, OnePackWithDuplicateVectors)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    cols->emplace_back(vec_cd);

    ColumnDefines read_cols = *cols;
    if (test_only_vec_column)
        read_cols = {vec_cd};

    // Prepare DMFile
    {
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 5);
        block.insert(createVecFloat32Column<Array>(
            {//
             {1.0, 2.0, 3.0},
             {1.0, 2.0, 3.0},
             {0.0, 0.0, 0.0},
             {1.0, 2.0, 3.0},
             {1.0, 2.0, 3.5}},
            vec_cd.name,
            vec_cd.id));
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();

    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(4);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.5}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(5, true), 0, 5))
                          .build2(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());

        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 1, 3, 4}),
                createVecFloat32Column<Array>({//
                                               {1.0, 2.0, 3.0},
                                               {1.0, 2.0, 3.0},
                                               {1.0, 2.0, 3.0},
                                               {1.0, 2.0, 3.5}}),
            }));
    }
}
CATCH

TEST_P(VectorIndexDMFileTest, MultiPacks)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    cols->emplace_back(vec_cd);

    ColumnDefines read_cols = *cols;
    if (test_only_vec_column)
        read_cols = {vec_cd};

    // Prepare DMFile
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
        block1.insert(
            createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}}, vec_cd.name, vec_cd.id));

        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
        block2.insert(
            createVecFloat32Column<Array>({{5.0, 5.0, 5.0}, {5.0, 5.0, 7.0}, {0.0, 0.0, 0.0}}, vec_cd.name, vec_cd.id));

        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->write(block2, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();

    // Pack #0 is filtered out according to VecIndex
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({5.0, 5.0, 5.5}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(6, true), 0, 6))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({3}),
                createVecFloat32Column<Array>({{5.0, 5.0, 5.0}}),
            }));
    }

    // Pack #1 is filtered out according to VecIndex
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({1.0, 2.0, 3.0}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(6, true), 0, 6))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}}),
            }));
    }

    // Both packs are reserved
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(2);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({0.0, 0.0, 0.0}));

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(std::make_shared<BitmapFilter>(6, true), 0, 6))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({1, 5}),
                createVecFloat32Column<Array>({{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}}),
            }));
    }

    // Pack Filter + MVCC (the matching row #5 is marked as filtered out by MVCC)
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(2);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({0.0, 0.0, 0.0}));

        auto bitmap_filter = std::make_shared<BitmapFilter>(6, true);
        bitmap_filter->set(/* start */ 5, /* limit */ 1, false);

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(bitmap_filter, 0, 6))
                          .tryBuildWithVectorIndex(
                              dm_file,
                              read_cols,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({0, 1}),
                createVecFloat32Column<Array>({{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}}),
            }));
    }
}
CATCH

TEST_P(VectorIndexDMFileTest, WithPackFilter)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
        .kind = tipb::VectorIndexKind::HNSW,
        .dimension = 1,
        .distance_metric = tipb::VectorDistanceMetric::L2,
    });
    cols->emplace_back(vec_cd);

    ColumnDefines read_cols = *cols;
    if (test_only_vec_column)
        read_cols = {vec_cd};

    // Prepare DMFile
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
        block1.insert(colVecFloat32("[0, 3)", vec_cd.name, vec_cd.id));

        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
        block2.insert(colVecFloat32("[3, 6)", vec_cd.name, vec_cd.id));

        Block block3 = DMTestEnv::prepareSimpleWriteBlockWithNullable(6, 9);
        block3.insert(colVecFloat32("[6, 9)", vec_cd.name, vec_cd.id));

        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->write(block2, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->write(block3, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();

    // Pack Filter using RowKeyRange
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(1);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({8.0}));

        // This row key range will cause pack#0 and pack#1 reserved, and pack#2 filtered out.
        auto row_key_ranges = RowKeyRanges{RowKeyRange::fromHandleRange(HandleRange(0, 5))};

        auto bitmap_filter = std::make_shared<BitmapFilter>(9, false);
        bitmap_filter->set(0, 6); // 0~6 rows are valid, 6~9 rows are invalid due to pack filter.

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(bitmap_filter, 0, 9))
                          .tryBuildWithVectorIndex(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({5}),
                createVecFloat32Column<Array>({{5.0}}),
            }));

        // TopK=4
        ann_query_info->set_top_k(4);
        builder = DMFileBlockInputStreamBuilder(dbContext());
        stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                     .setBitmapFilter(BitmapFilterView(bitmap_filter, 0, 9))
                     .tryBuildWithVectorIndex(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({2, 3, 4, 5}),
                createVecFloat32Column<Array>({{2.0}, {3.0}, {4.0}, {5.0}}),
            }));
    }

    // Pack Filter + Bitmap Filter
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_cd.id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(3);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32({8.0}));

        // This row key range will cause pack#0 and pack#1 reserved, and pack#2 filtered out.
        auto row_key_ranges = RowKeyRanges{RowKeyRange::fromHandleRange(HandleRange(0, 5))};

        // Valid rows are 0, 1, , 3, 4
        auto bitmap_filter = std::make_shared<BitmapFilter>(9, false);
        bitmap_filter->set(0, 2);
        bitmap_filter->set(3, 2);

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setRSOperator(wrapWithANNQueryInfo(nullptr, ann_query_info))
                          .setBitmapFilter(BitmapFilterView(bitmap_filter, 0, 9))
                          .tryBuildWithVectorIndex(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                createColumn<Int64>({1, 3, 4}),
                createVecFloat32Column<Array>({{1.0}, {3.0}, {4.0}}),
            }));
    }
}
CATCH

class VectorIndexSegmentTestBase
    : public VectorIndexTestUtils
    , public SegmentTestBasic
{
public:
    BlockInputStreamPtr annQuery(
        PageIdU64 segment_id,
        Int64 begin,
        Int64 end,
        ColumnDefines columns_to_read,
        UInt32 top_k,
        const std::vector<Float32> & ref_vec)
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_column_id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(top_k);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32(ref_vec));
        return read(segment_id, begin, end, columns_to_read, ann_query_info);
    }

    BlockInputStreamPtr annQuery(
        PageIdU64 segment_id,
        ColumnDefines columns_to_read,
        UInt32 top_k,
        const std::vector<Float32> & ref_vec)
    {
        auto [segment_start_key, segment_end_key] = getSegmentKeyRange(segment_id);
        return annQuery(segment_id, segment_start_key, segment_end_key, columns_to_read, top_k, ref_vec);
    }

    BlockInputStreamPtr read(
        PageIdU64 segment_id,
        Int64 begin,
        Int64 end,
        ColumnDefines columns_to_read,
        ANNQueryInfoPtr ann_query)
    {
        auto range = buildRowKeyRange(begin, end);
        auto [segment, snapshot] = getSegmentForRead(segment_id);
        auto stream = segment->getBitmapFilterInputStream(
            *dm_context,
            columns_to_read,
            snapshot,
            {range},
            std::make_shared<PushDownFilter>(wrapWithANNQueryInfo({}, ann_query)),
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_SIZE);
        return stream;
    }

    ColumnDefine cdPK() { return getExtraHandleColumnDefine(options.is_common_handle); }

protected:
    Block prepareWriteBlockImpl(Int64 start_key, Int64 end_key, bool is_deleted) override
    {
        auto block = SegmentTestBasic::prepareWriteBlockImpl(start_key, end_key, is_deleted);
        block.insert(colVecFloat32(fmt::format("[{}, {})", start_key, end_key), vec_column_name, vec_column_id));
        return block;
    }

    void prepareColumns(const ColumnDefinesPtr & columns) override
    {
        auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
        vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });
        columns->emplace_back(vec_cd);
    }

protected:
    // DMFile has different logic when there is only vec column.
    // So we test it independently.
    bool test_only_vec_column = false;
    int pack_size = 10;

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns) const
    {
        if (!test_only_vec_column)
            return columns;

        // In test_only_vec_column mode, only contains the Array column.
        for (const auto & col : columns)
        {
            if (col.type->getName() == "Array(Float32)")
                return {col};
        }

        RUNTIME_CHECK(false);
    }

    virtual Strings createColumnNames()
    {
        if (!test_only_vec_column)
            return {DMTestEnv::pk_name, vec_column_name};

        // In test_only_vec_column mode, only contains the Array column.
        return {vec_column_name};
    }

    virtual ColumnDefines createQueryColumns()
    {
        if (!test_only_vec_column)
            return {cdPK(), cdVec()};

        return {cdVec()};
    }

    inline void assertStreamOut(BlockInputStreamPtr stream, std::string_view expected_sequence)
    {
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            createColumnNames(),
            createColumnData({
                colInt64(expected_sequence),
                colVecFloat32(expected_sequence),
            }));
    }
};

class VectorIndexSegmentTest1
    : public VectorIndexSegmentTestBase
    , public testing::WithParamInterface<bool>
{
public:
    VectorIndexSegmentTest1() { test_only_vec_column = GetParam(); }
};

INSTANTIATE_TEST_CASE_P( //
    VectorIndex,
    VectorIndexSegmentTest1,
    /* vec_only */ ::testing::Bool());

class VectorIndexSegmentTest2
    : public VectorIndexSegmentTestBase
    , public testing::WithParamInterface<std::tuple<bool, int>>
{
public:
    VectorIndexSegmentTest2() { std::tie(test_only_vec_column, pack_size) = GetParam(); }
};

INSTANTIATE_TEST_CASE_P( //
    VectorIndex,
    VectorIndexSegmentTest2,
    ::testing::Combine( //
        /* vec_only */ ::testing::Bool(),
        /* pack_size */ ::testing::Values(1, 2, 3, 4, 5)));

TEST_P(VectorIndexSegmentTest1, DataInCFInMemory)
try
{
    // Vector in memory will not filter by ANNQuery at all.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0);
    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 10);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)|[10, 15)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ -10);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)|[10, 15)|[-10, -5)");
}
CATCH

TEST_P(VectorIndexSegmentTest1, DataInCFTiny)
try
{
    // Vector in column file tiny will not filter by ANNQuery at all.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ -10);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)|[-10, -5)");

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)|[-10, -5)");

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 12, /* at */ -10);
    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[2, 5)|[-10, 2)");
}
CATCH

TEST_P(VectorIndexSegmentTest1, DataInCFBig)
try
{
    // Vector in column file big will not filter by ANNQuery at all.
    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[0, 5)");
}
CATCH

TEST_P(VectorIndexSegmentTest2, DataInStable)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size;
    reloadDMContext();

    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[4, 5)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 3, {100.0});
    assertStreamOut(stream, "[2, 5)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {1.1});
    assertStreamOut(stream, "[1, 2)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 2, {1.1});
    assertStreamOut(stream, "[1, 3)");
}
CATCH

TEST_P(VectorIndexSegmentTest2, DataInStableAndDelta)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size;
    reloadDMContext();

    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 20);

    // ANNQuery will be only effective to Stable layer. All delta data will be returned.

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[4, 5)|[20, 30)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 2, {10.0});
    assertStreamOut(stream, "[3, 5)|[20, 30)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 5, {10.0});
    assertStreamOut(stream, "[0, 5)|[20, 30)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 10, {10.0});
    assertStreamOut(stream, "[0, 5)|[20, 30)");
}
CATCH

TEST_P(VectorIndexSegmentTest2, SegmentSplit)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size;
    reloadDMContext();

    // Stable: [0, 10), [20, 30)
    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 0, /* clear */ false);
    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 20, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // Delta: [12, 18), [50, 60)
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 6, /* at */ 12);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 50);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto right_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 15, Segment::SplitMode::Logical);
    RUNTIME_CHECK(right_seg_id.has_value());

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[9, 10)|[12, 15)");

    stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 100, {100.0});
    assertStreamOut(stream, "[0, 10)|[12, 15)");

    stream = annQuery(right_seg_id.value(), createQueryColumns(), 1, {100.0});
    assertStreamOut(stream, "[29, 30)|[15, 18)|[50, 60)");

    stream = annQuery(right_seg_id.value(), createQueryColumns(), 100, {100.0});
    assertStreamOut(stream, "[20, 30)|[15, 18)|[50, 60)");
}
CATCH

class VectorIndexSegmentExtraColumnTest
    : public VectorIndexSegmentTestBase
    , public testing::WithParamInterface<std::tuple<bool, int>>
{
public:
    VectorIndexSegmentExtraColumnTest() { std::tie(test_only_vec_column, pack_size) = GetParam(); }

protected:
    const String extra_column_name = "extra";
    const ColumnID extra_column_id = 500;

    ColumnDefine cdExtra()
    {
        // When used in read, no need to assign vector_index.
        return ColumnDefine(extra_column_id, extra_column_name, tests::typeFromString("Int64"));
    }

    Block prepareWriteBlockImpl(Int64 start_key, Int64 end_key, bool is_deleted) override
    {
        auto block = VectorIndexSegmentTestBase::prepareWriteBlockImpl(start_key, end_key, is_deleted);
        block.insert(
            colInt64(fmt::format("[{}, {})", start_key + 1000, end_key + 1000), extra_column_name, extra_column_id));
        return block;
    }

    void prepareColumns(const ColumnDefinesPtr & columns) override
    {
        VectorIndexSegmentTestBase::prepareColumns(columns);
        columns->emplace_back(cdExtra());
    }

    Strings createColumnNames() override
    {
        if (!test_only_vec_column)
            return {DMTestEnv::pk_name, vec_column_name, extra_column_name};

        // In test_only_vec_column mode, only contains the Array column.
        return {vec_column_name};
    }

    ColumnDefines createQueryColumns() override
    {
        if (!test_only_vec_column)
            return {cdPK(), cdVec(), cdExtra()};

        return {cdVec()};
    }
};

INSTANTIATE_TEST_CASE_P(
    VectorIndex,
    VectorIndexSegmentExtraColumnTest,
    ::testing::Combine( //
        /* vec_only */ ::testing::Bool(),
        /* pack_size */ ::testing::Values(1 /*, 2, 3, 4, 5*/)));

TEST_P(VectorIndexSegmentExtraColumnTest, DataInStableAndDelta)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size;
    reloadDMContext();

    ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 5, /* at */ 0, /* clear */ false);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 20);

    auto stream = annQuery(DELTA_MERGE_FIRST_SEGMENT_ID, createQueryColumns(), 1, {100.0});
    ASSERT_INPUTSTREAM_COLS_UR(
        stream,
        createColumnNames(),
        createColumnData({
            colInt64("[4, 5)|[20, 30)"),
            colVecFloat32("[4, 5)|[20, 30)"),
            colInt64("[1004, 1005)|[1020, 1030)"),
        }));
}
CATCH

class VectorIndexSegmentOnS3Test
    : public VectorIndexTestUtils
    , public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        FailPointHelper::enableFailPoint(FailPoints::force_use_dmfile_format_v3);

        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        TiFlashStorageTestBasic::SetUp();

        auto & global_context = TiFlashTestEnv::getGlobalContext();

        global_context.getSharedContextDisagg()->initRemoteDataStore(
            global_context.getFileProvider(),
            /*s3_enabled*/ true);
        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store != nullptr);

        orig_mode = global_context.getPageStorageRunMode();
        global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
        global_context.tryReleaseWriteNodePageStorageForTest();
        global_context.initializeWriteNodePageStorageIfNeed(global_context.getPathPool());

        global_context.setVectorIndexCache(1000);

        auto kvstore = db_context->getTMTContext().getKVStore();
        {
            auto meta_store = metapb::Store{};
            meta_store.set_id(100);
            kvstore->setStore(meta_store);
        }

        TiFlashStorageTestBasic::reload(DB::Settings());
        storage_path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        page_id_allocator = std::make_shared<GlobalPageIdAllocator>();
        storage_pool = std::make_shared<StoragePool>(
            *db_context,
            NullspaceID,
            ns_id,
            *storage_path_pool,
            page_id_allocator,
            "test.t1");
        storage_pool->restore();

        StorageRemoteCacheConfig file_cache_config{
            .dir = fmt::format("{}/fs_cache", getTemporaryPath()),
            .capacity = 1 * 1000 * 1000 * 1000,
        };
        FileCache::initialize(global_context.getPathCapacity(), file_cache_config);

        auto cols = DMTestEnv::getDefaultColumns();
        auto vec_cd = cdVec();
        vec_cd.vector_index = std::make_shared<TiDB::VectorIndexDefinition>(TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });
        cols->emplace_back(vec_cd);
        setColumns(cols);

        auto dm_context = dmContext();
        wn_segment = Segment::newSegment(
            Logger::get(),
            *dm_context,
            table_columns,
            RowKeyRange::newAll(false, 1),
            DELTA_MERGE_FIRST_SEGMENT_ID,
            0);
        ASSERT_EQ(wn_segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    void TearDown() override
    {
        FailPointHelper::disableFailPoint(FailPoints::force_use_dmfile_format_v3);

        FileCache::shutdown();

        auto & global_context = TiFlashTestEnv::getGlobalContext();
        global_context.dropVectorIndexCache();
        global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        global_context.setPageStorageRunMode(orig_mode);

        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

    static ColumnDefine cdPK() { return getExtraHandleColumnDefine(false); }

    BlockInputStreamPtr createComputeNodeStream(
        const SegmentPtr & write_node_segment,
        const ColumnDefines & columns_to_read,
        const PushDownFilterPtr & filter,
        const ScanContextPtr & read_scan_context = nullptr)
    {
        auto write_dm_context = dmContext();
        auto snap = write_node_segment->createSnapshot(*write_dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
        auto snap_proto = Remote::Serializer::serializeSegment(
            snap,
            write_node_segment->segmentId(),
            0,
            write_node_segment->rowkey_range,
            {write_node_segment->rowkey_range},
            dummy_mem_tracker,
            /*need_mem_data*/ true);

        auto cn_segment = std::make_shared<Segment>(
            Logger::get(),
            /*epoch*/ 0,
            write_node_segment->getRowKeyRange(),
            write_node_segment->segmentId(),
            /*next_segment_id*/ 0,
            nullptr,
            nullptr);

        auto read_dm_context = dmContext(read_scan_context);
        auto cn_segment_snap = Remote::Serializer::deserializeSegment(
            *read_dm_context,
            /* store_id */ 100,
            /* keyspace_id */ 0,
            /* table_id */ 100,
            snap_proto);

        auto stream = cn_segment->getInputStream(
            ReadMode::Bitmap,
            *read_dm_context,
            columns_to_read,
            cn_segment_snap,
            {write_node_segment->getRowKeyRange()},
            filter,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);

        return stream;
    }

    static void removeAllFileCache()
    {
        auto * file_cache = FileCache::instance();
        auto file_segments = file_cache->getAll();
        for (const auto & file_seg : file_cache->getAll())
            file_cache->remove(file_cache->toS3Key(file_seg->getLocalFileName()), true);

        RUNTIME_CHECK(file_cache->getAll().empty());
    }

    void prepareWriteNodeStable()
    {
        auto dm_context = dmContext();
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 100);
        block.insert(colVecFloat32("[0, 100)", vec_column_name, vec_column_id));
        wn_segment->write(*dm_context, std::move(block), true);
        wn_segment = wn_segment->mergeDelta(*dm_context, tableColumns());

        // Let's just make sure we are later indeed reading from S3
        RUNTIME_CHECK(wn_segment->stable->getDMFiles()[0]->path().rfind("s3://") == 0);
    }

    BlockInputStreamPtr computeNodeTableScan()
    {
        return createComputeNodeStream(wn_segment, {cdPK(), cdVec()}, nullptr);
    }

    BlockInputStreamPtr computeNodeANNQuery(
        const std::vector<Float32> ref_vec,
        UInt32 top_k = 1,
        const ScanContextPtr & read_scan_context = nullptr)
    {
        auto ann_query_info = std::make_shared<tipb::ANNQueryInfo>();
        ann_query_info->set_column_id(vec_column_id);
        ann_query_info->set_distance_metric(tipb::VectorDistanceMetric::L2);
        ann_query_info->set_top_k(top_k);
        ann_query_info->set_ref_vec_f32(encodeVectorFloat32(ref_vec));

        auto stream = createComputeNodeStream(
            wn_segment,
            {cdPK(), cdVec()},
            std::make_shared<PushDownFilter>(wrapWithANNQueryInfo(nullptr, ann_query_info)),
            read_scan_context);
        return stream;
    }

protected:
    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns) { table_columns = columns; }

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContextPtr dmContext(const ScanContextPtr & scan_context = nullptr)
    {
        return DMContext::createUnique(
            *db_context,
            storage_path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            false,
            1,
            db_context->getSettingsRef(),
            scan_context);
    }

protected:
    /// all these var lives as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;

    NamespaceID ns_id = 100;

    // the segment we are going to test
    SegmentPtr wn_segment;

    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;

    // MemoryTrackerPtr memory_tracker;
    MemTrackerWrapper dummy_mem_tracker = MemTrackerWrapper(0, root_of_query_mem_trackers.get());
};

TEST_F(VectorIndexSegmentOnS3Test, FileCacheNotEnabled)
try
{
    prepareWriteNodeStable();

    FileCache::shutdown();
    auto stream = computeNodeANNQuery({5.0});

    try
    {
        stream->readPrefix();
        stream->read();
        FAIL();
    }
    catch (const DB::Exception & ex)
    {
        ASSERT_STREQ("Check file_cache failed: Must enable S3 file cache to use vector index", ex.message().c_str());
    }
    catch (...)
    {
        FAIL();
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, ReadWithoutIndex)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto stream = computeNodeTableScan();
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[0, 100)"),
                colVecFloat32("[0, 100)"),
            }));
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, ReadFromIndex)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));
    }
    {
        // Read again, we should be reading from memory cache.

        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 1);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 0);
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, FileCacheEvict)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));
    }
    {
        // Simulate cache evict.
        removeAllFileCache();
    }
    {
        // Check whether on-disk file is successfully unlinked when there is a memory
        // cache.
        auto * file_cache = FileCache::instance();
        ASSERT_TRUE(std::filesystem::is_empty(file_cache->cache_dir));
    }
    {
        // When cache is evicted (but memory cache exists), the query should be fine.
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        // Read again, we should be reading from memory cache.

        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 1);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 0);
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, FileCacheEvictAndVectorCacheDrop)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));
    }
    {
        // Simulate cache evict.
        removeAllFileCache();
    }
    {
        // Check whether on-disk file is successfully unlinked when there is a memory
        // cache.
        auto * file_cache = FileCache::instance();
        ASSERT_TRUE(std::filesystem::is_empty(file_cache->cache_dir));
    }
    {
        // We should be able to clear something from the vector index cache.
        auto vec_cache = TiFlashTestEnv::getGlobalContext().getVectorIndexCache();
        ASSERT_EQ(1, vec_cache->cleanOutdatedCacheEntries());
    }
    {
        // When cache is evicted (and memory cache is dropped), the query should be fine.
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        // Read again, we should be reading from memory cache.

        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 1);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 0);
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, FileCacheDeleted)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));

        // Simulate cache file is deleted by user.
        std::filesystem::remove_all(file_cache->cache_dir);
    }
    {
        // Query should be fine.
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        // Read again, we should be reading from memory cache.

        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 1);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 0);
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, FileCacheDeletedAndVectorCacheDrop)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
        ASSERT_FALSE(std::filesystem::is_empty(file_cache->cache_dir));

        // Simulate cache file is deleted by user.
        std::filesystem::remove_all(file_cache->cache_dir);
    }
    {
        // We should be able to clear something from the vector index cache.
        auto vec_cache = TiFlashTestEnv::getGlobalContext().getVectorIndexCache();
        ASSERT_EQ(1, vec_cache->cleanOutdatedCacheEntries());
    }
    {
        // Query should be fine.
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);
    }
    {
        // Read again, we should be reading from memory cache.

        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 1);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 0);
    }
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, ConcurrentDownloadFromS3)
try
{
    prepareWriteNodeStable();
    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }

    auto sp_s3_fg_download = SyncPointCtl::enableInScope("FileCache::fgDownload");
    auto sp_wait_other_s3 = SyncPointCtl::enableInScope("before_FileSegment::waitForNotEmpty_wait");

    auto th_1 = std::async([&]() {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[5, 6)"),
                colVecFloat32("[5, 6)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);

        ASSERT_EQ(PerfContext::file_cache.fg_download_from_s3, 1);
        ASSERT_EQ(PerfContext::file_cache.fg_wait_download_from_s3, 0);
    });

    // th_1 should be blocked when downloading from s3.
    sp_s3_fg_download.waitAndPause();

    auto th_2 = std::async([&]() {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({7.0}, 1, scan_context);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, vec_column_name}),
            createColumns({
                colInt64("[7, 8)"),
                colVecFloat32("[7, 8)"),
            }));

        ASSERT_EQ(scan_context->total_vector_idx_load_from_cache, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_disk, 0);
        ASSERT_EQ(scan_context->total_vector_idx_load_from_s3, 1);

        ASSERT_EQ(PerfContext::file_cache.fg_download_from_s3, 0);
        ASSERT_EQ(PerfContext::file_cache.fg_wait_download_from_s3, 1);
    });

    // th_2 should be blocked by waiting th_1 to finish downloading from s3.
    sp_wait_other_s3.waitAndNext();

    // Let th_1 finish downloading from s3.
    sp_s3_fg_download.next();

    // Both th_1 and th_2 should be able to finish without hitting sync points again.
    // e.g. th_2 should not ever try to fgDownload.
    th_1.get();
    th_2.get();
}
CATCH

TEST_F(VectorIndexSegmentOnS3Test, S3Failure)
try
{
    prepareWriteNodeStable();
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::file_cache_fg_download_fail);
    SCOPE_EXIT({ DB::FailPointHelper::disableFailPoint(DB::FailPoints::file_cache_fg_download_fail); });

    {
        auto * file_cache = FileCache::instance();
        ASSERT_EQ(0, file_cache->getAll().size());
    }
    {
        auto scan_context = std::make_shared<ScanContext>();
        auto stream = computeNodeANNQuery({5.0}, 1, scan_context);

        ASSERT_THROW(
            {
                stream->readPrefix();
                stream->read();
            },
            DB::Exception);
    }
}
CATCH


} // namespace DB::DM::tests

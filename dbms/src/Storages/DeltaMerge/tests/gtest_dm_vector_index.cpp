// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,n
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

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
        dm_context = std::make_unique<DMContext>(
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
        return DMFile::restore(file_provider, file_id, page_id, parent_path, DMFile::ReadMetaMode::all());
    }

    Context & dbContext() { return *db_context; }

protected:
    std::unique_ptr<DMContext> dm_context{};
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool{};
    std::shared_ptr<StoragePool> storage_pool{};

protected:
    String parent_path;
    DMFilePtr dm_file = nullptr;

public:
    VectorIndexDMFileTest() { test_only_vec_column = GetParam(); }

protected:
    // DMFile has different logic when there is only vec column.
    // So we test it independently.
    bool test_only_vec_column = false;

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns)
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
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexInfo>(TiDB::VectorIndexInfo{
        .kind = TiDB::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = TiDB::DistanceMetric::L2,
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
            ASSERT_STREQ("Query distance metric Cosine does not match index distance metric L2", ex.message().c_str());
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
                          .build2(
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

TEST_P(VectorIndexDMFileTest, MultiPacks)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto vec_cd = ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexInfo>(TiDB::VectorIndexInfo{
        .kind = TiDB::VectorIndexKind::HNSW,
        .dimension = 3,
        .distance_metric = TiDB::DistanceMetric::L2,
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
                          .build2(
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
    vec_cd.vector_index = std::make_shared<TiDB::VectorIndexInfo>(TiDB::VectorIndexInfo{
        .kind = TiDB::VectorIndexKind::HNSW,
        .dimension = 1,
        .distance_metric = TiDB::DistanceMetric::L2,
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
                          .build2(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
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
                     .build2(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
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
                          .build2(dm_file, read_cols, row_key_ranges, std::make_shared<ScanContext>());
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

    ColumnDefine cdVec()
    {
        // When used in read, no need to assign vector_index.
        return ColumnDefine(vec_column_id, vec_column_name, tests::typeFromString("Array(Float32)"));
    }

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
        vec_cd.vector_index = std::make_shared<TiDB::VectorIndexInfo>(TiDB::VectorIndexInfo{
            .kind = TiDB::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = TiDB::DistanceMetric::L2,
        });
        columns->emplace_back(vec_cd);
    }

protected:
    // DMFile has different logic when there is only vec column.
    // So we test it independently.
    bool test_only_vec_column = false;
    int pack_size = 10;

    ColumnsWithTypeAndName createColumnData(const ColumnsWithTypeAndName & columns)
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

} // namespace DB::DM::tests

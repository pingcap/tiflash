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

#include <Columns/IColumn.h>
#include <Common/FailPoint.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TiDB/Schema/TiDB.h>

#include <ext/scope_guard.h>
#include <limits>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_drop_segment[];
extern const char exception_after_drop_segment[];
} // namespace FailPoints
namespace DM
{
namespace tests
{
TEST(StorageDeltaMergeTest, ReadWriteCase1)
try
{
    size_t num_rows_write = 100;
    // prepare block data
    Block sample;
    sample.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows_write, /*reversed*/ true), "col1"));
    sample.insert(DB::tests::createColumn<String>(Strings(num_rows_write, "a"), "col2"));

    auto ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    // create table
    {
        NamesAndTypesList names_and_types_list{
            {"col1", std::make_shared<DataTypeInt64>()},
            {"col2", std::make_shared<DataTypeString>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath("StorageDeltaMerge_ReadWriteCase1");
        if (Poco::File path(path_name); path.exists())
            path.remove(true);

        // primary_expr_ast
        const String table_name = "t_1233";
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create(
            "TiFlash",
            /* db_name= */ "default",
            table_name,
            std::nullopt,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            *ctx);
        storage->startup();
    }

    // test writing to DeltaMergeStorage
    {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx->getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    auto scan_context = std::make_shared<DM::ScanContext>();
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(
        ctx->getSettingsRef().resolve_locks,
        std::numeric_limits<UInt64>::max(),
        scan_context);
    // these field should live long enough because `DAGQueryInfo` only
    // keep a ref on them
    const google::protobuf::RepeatedPtrField<tipb::Expr> filters{};
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
    TiDB::ColumnInfos source_columns{};
    const std::vector<int> runtime_filter_ids;
    query_info.dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        pushed_down_filters, // Not care now
        source_columns, // Not care now
        runtime_filter_ids,
        0,
        ctx->getTimezoneInfo());
    BlockInputStreams ins = storage->read(column_names, query_info, *ctx, stage2, 8192, 1);
    ASSERT_EQ(ins.size(), 1);
    BlockInputStreamPtr in = ins[0];
    ASSERT_INPUTSTREAM_BLOCK_UR(
        in,
        Block(
            {createColumn<Int64>(createNumbers<Int64>(0, num_rows_write), "col1"),
             createColumn<String>(Strings(num_rows_write, "a"), "col2")}));

    auto store_status = storage->status();
    Block status = store_status->read();
    ColumnPtr col_name = status.getByName("Name").column;
    ColumnPtr col_value = status.getByName("Value").column;
    for (size_t i = 0; i < col_name->size(); i++)
    {
        if (col_name->getDataAt(i) == String("segment_count"))
        {
            EXPECT_EQ(col_value->getDataAt(i), String(DB::toString(1)));
        }
        else if (col_name->getDataAt(i) == String("total_rows"))
        {
            EXPECT_EQ(col_value->getDataAt(i), String(DB::toString(num_rows_write)));
        }
    }
    auto delta_store = storage->getStore();
    size_t total_segment_rows = 0;
    auto segment_stats = delta_store->getSegmentsStats();
    for (auto & stat : segment_stats)
    {
        total_segment_rows += stat.rows;
    }
    EXPECT_EQ(total_segment_rows, num_rows_write);
    storage->drop();
    // remove the storage from TiFlash context manually
    storage->removeFromTMTContext();
}
CATCH

TEST(StorageDeltaMergeTest, Rename)
try
{
    auto ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath("StorageDeltaMerge_Rename");
    const String table_name = "t_1234";
    const String db_name = "default";
    // create table
    {
        NamesAndTypesList names_and_types_list{
            //{"col1", std::make_shared<DataTypeUInt64>()},
            {"col1", std::make_shared<DataTypeInt64>()},
            {"col2", std::make_shared<DataTypeString>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        if (Poco::File path(path_name); path.exists())
        {
            path.remove(true);
        }

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create(
            "TiFlash",
            db_name,
            table_name,
            std::nullopt,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            *ctx);
        storage->startup();
    }

    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getTableName(), table_name);
    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getDatabaseName(), db_name);
    ASSERT_FALSE(storage->storeInited());

    // Rename database name before store object is created.
    const String new_db_name = "new_" + storage->getDatabaseName();
    const String new_display_table_name = "new_" + storage->getTableName();
    storage->rename(path_name, new_db_name, table_name, new_display_table_name);
    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getTableName(), table_name);
    ASSERT_EQ(storage->getDatabaseName(), new_db_name);
    ASSERT_EQ(storage->getTableInfo().name, new_display_table_name);

    // prepare block data
    Block sample;
    sample.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(0, 100, /*reversed*/ true), "col1"));
    sample.insert(DB::tests::createColumn<String>(Strings(100, "a"), "col2"));
    // Writing will create store object.
    {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx->getSettingsRef());
        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
        ASSERT_TRUE(storage->storeInited());
    }

    // TiFlash always use t_{table_id} as table name
    storage->rename(path_name, new_db_name, table_name, table_name);
    ASSERT_EQ(storage->getTableName(), table_name);
    ASSERT_EQ(storage->getDatabaseName(), new_db_name);

    storage->drop();
    // remove the storage from TiFlash context manually
    storage->removeFromTMTContext();
}
CATCH

TEST(StorageDeltaMergeTest, HandleCol)
try
{
    auto ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath("StorageDeltaMerge_HandleCol");
    const String table_name = "t_1235";
    const String db_name = "default";
    // create table
    {
        NamesAndTypesList names_and_types_list{
            {"col1", std::make_shared<DataTypeInt64>()},
            {"col2", std::make_shared<DataTypeString>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        if (Poco::File path(path_name); path.exists())
        {
            path.remove(true);
        }

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create(
            "TiFlash",
            db_name,
            table_name,
            std::nullopt,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            *ctx);
        storage->startup();
    }

    ASSERT_FALSE(storage->storeInited());
    auto pk_type = storage->getPKTypeImpl();
    auto sort_desc = storage->getPrimarySortDescription();
    ASSERT_FALSE(storage->storeInited());

    const auto & store = storage->getStore();
    ASSERT_TRUE(storage->storeInited());
    auto pk_type2 = store->getPKDataType();
    auto sort_desc2 = store->getPrimarySortDescription();

    ASSERT_EQ(pk_type->getTypeId(), pk_type2->getTypeId());
    ASSERT_EQ(sort_desc.size(), 1);
    ASSERT_EQ(sort_desc2.size(), 1);
    ASSERT_EQ(sort_desc.front().column_name, sort_desc2.front().column_name);
    ASSERT_EQ(sort_desc.front().direction, sort_desc2.front().direction);
    ASSERT_EQ(sort_desc.front().nulls_direction, sort_desc2.front().nulls_direction);

    storage->drop();
    // remove the storage from TiFlash context manually
    storage->removeFromTMTContext();
}
CATCH

TEST(StorageDeltaMergeInternalTest, GetMergedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = GET_REGION_RANGE(100, 200, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(200, 250, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(300, 400, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(425, 475, 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 3);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), ::DB::DM::HandleRange(100, 250));
    ASSERT_RANGE_EQ(ranges[1].toHandleRange(), ::DB::DM::HandleRange(300, 400));
    ASSERT_RANGE_EQ(ranges[2].toHandleRange(), ::DB::DM::HandleRange(425, 475));
}

TEST(StorageDeltaMergeInternalTest, GetMergedQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(200, 250, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2).toRegionRange(1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, true, 2);
    ASSERT_EQ(ranges.size(), 3);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(100, 250, 2));
    ASSERT_ROWKEY_RANGE_EQ(ranges[1], DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2));
    ASSERT_ROWKEY_RANGE_EQ(ranges[2], DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2));
}

TEST(StorageDeltaMergeInternalTest, MergedUnsortedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = GET_REGION_RANGE(2360148, 2456148, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(1961680, 2057680, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2264148, 2360148, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2057680, 2153680, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2153680, 2264148, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2552148, 2662532, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2758532, 2854532, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2854532, 2950532, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2456148, 2552148, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(2662532, 2758532, 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 1);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), DB::DM::HandleRange(1961680, 2950532));
}

TEST(StorageDeltaMergeInternalTest, MergedUnsortedQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2360148, 2456148, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(1961680, 2057680, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2264148, 2360148, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2057680, 2153680, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2153680, 2264148, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2552148, 2662532, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2758532, 2854532, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2854532, 2950532, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2456148, 2552148, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(2662532, 2758532, 2).toRegionRange(1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, true, 2);
    ASSERT_EQ(ranges.size(), 1);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(1961680, 2950532, 2));
}

TEST(StorageDeltaMergeInternalTest, GetFullQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table
        = GET_REGION_RANGE(std::numeric_limits<HandleID>::min(), std::numeric_limits<HandleID>::max(), 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 1);
    const auto full_range = ::DB::DM::HandleRange::newAll();
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), full_range);
}

TEST(StorageDeltaMergeInternalTest, OverlapQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = GET_REGION_RANGE(100, 200, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(150, 250, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(300, 400, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(425, 475, 1);
    regions.emplace_back(region);

    // Overlaped ranges throw exception
    ASSERT_ANY_THROW(auto ranges = ::DB::getQueryRanges(regions, 1, false, 1));
}

TEST(StorageDeltaMergeInternalTest, OverlapQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(150, 250, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2).toRegionRange(1);
    regions.emplace_back(region);

    // Overlaped ranges throw exception
    ASSERT_ANY_THROW(auto ranges = ::DB::getQueryRanges(regions, 1, true, 2));
}

TEST(StorageDeltaMergeInternalTest, WeirdRange)
{
    // [100, 200), [200, MAX]
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = GET_REGION_RANGE(100, 200, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(200, std::numeric_limits<HandleID>::max(), 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 1);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), DB::DM::HandleRange(100, DB::DM::HandleRange::MAX));
}

TEST(StorageDeltaMergeInternalTest, WeirdRangeCommonHandle)
{
    // [100, 200), [200, MAX), [MAX, MAX)
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region(1, 1, 1, 1);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(
                                std::numeric_limits<HandleID>::max(),
                                std::numeric_limits<HandleID>::max(),
                                2)
                                .toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table
        = DMTestEnv::getRowKeyRangeForClusteredIndex(200, std::numeric_limits<HandleID>::max(), 2).toRegionRange(1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, true, 2);
    ASSERT_EQ(ranges.size(), 1);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(100, DB::DM::HandleRange::MAX, 2));
}

TEST(StorageDeltaMergeInternalTest, RangeSplit)
{
    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo region(1, 1, 1, 1);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(200, 300, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2).toRegionRange(1);
        regions.emplace_back(region);

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 0);
            ASSERT_EQ(ranges.size(), 2);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 1);
            ASSERT_EQ(ranges.size(), 2);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 2);
            ASSERT_EQ(ranges.size(), 2);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 3);
            ASSERT_EQ(ranges.size(), 3);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 4);
            ASSERT_EQ(ranges.size(), 4);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 5);
            ASSERT_EQ(ranges.size(), 4);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 100);
            ASSERT_EQ(ranges.size(), 4);
        }
    }

    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo region(1, 1, 1, 1);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(0, 100, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(200, 300, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2).toRegionRange(1);
        regions.emplace_back(region);
        region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2).toRegionRange(1);
        regions.emplace_back(region);

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 0);
            ASSERT_EQ(ranges.size(), 3);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 1);
            ASSERT_EQ(ranges.size(), 3);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 2);
            ASSERT_EQ(ranges.size(), 3);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 3);
            ASSERT_EQ(ranges.size(), 3);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 4);
            ASSERT_EQ(ranges.size(), 4);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 5);
            ASSERT_EQ(ranges.size(), 4);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 100);
            ASSERT_EQ(ranges.size(), 4);
        }
    }
}

TEST(StorageDeltaMergeTest, ReadExtraPhysicalTableID)
try
{
    // prepare block data
    size_t num_rows_write = 100;
    Block sample;
    sample.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows_write, /*reversed*/ true), "col1"));
    sample.insert(DB::tests::createColumn<String>(Strings(num_rows_write, "a"), "col2"));
    constexpr TiDB::TableID table_id = 1;
    const String table_name = fmt::format("t_{}", table_id);

    auto ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    // create table
    {
        NamesAndTypesList names_and_types_list{
            {"col1", std::make_shared<DataTypeInt64>()},
            {"col2", std::make_shared<DataTypeString>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        const String path_name
            = DB::tests::TiFlashTestEnv::getTemporaryPath("StorageDeltaMerge_ReadExtraPhysicalTableID");
        if (Poco::File path(path_name); path.exists())
            path.remove(true);

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        TiDB::TableInfo tidb_table_info;
        tidb_table_info.id = table_id;

        storage = StorageDeltaMerge::create(
            "TiFlash",
            /* db_name= */ "default",
            table_name,
            tidb_table_info,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            *ctx);
        storage->startup();
    }

    // test writing to DeltaMergeStorage
    {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx->getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    auto scan_context = std::make_shared<DM::ScanContext>();
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(
        ctx->getSettingsRef().resolve_locks,
        std::numeric_limits<UInt64>::max(),
        scan_context);
    // these field should live long enough because `DAGQueryInfo` only
    // keep a ref on them
    const google::protobuf::RepeatedPtrField<tipb::Expr> filters{};
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
    TiDB::ColumnInfos source_columns{};
    const std::vector<int> runtime_filter_ids;
    query_info.dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        pushed_down_filters, // Not care now
        source_columns, // Not care now
        runtime_filter_ids,
        0,
        ctx->getTimezoneInfo());
    Names read_columns = {"col1", EXTRA_TABLE_ID_COLUMN_NAME, "col2"};
    BlockInputStreams ins = storage->read(read_columns, query_info, *ctx, stage2, 8192, 1);
    ASSERT_EQ(ins.size(), 1);
    BlockInputStreamPtr in = ins[0];
    ASSERT_INPUTSTREAM_BLOCK_UR(
        in,
        Block({
            createColumn<Int64>(createNumbers<Int64>(0, num_rows_write), "col1"),
            createConstColumn<Nullable<Int64>>(num_rows_write, table_id, EXTRA_TABLE_ID_COLUMN_NAME),
            createColumn<String>(Strings(num_rows_write, "a"), "col2"),
        }));

    storage->drop();
    // remove the storage from TiFlash context manually
    storage->removeFromTMTContext();
}
CATCH

TEST(StorageDeltaMergeTest, RestoreAfterClearData)
try
{
    auto & global_settings = ::DB::tests::TiFlashTestEnv::getGlobalContext().getSettingsRef();
    // store the old value to restore global_context settings after the test finish to avoid influence other tests
    auto old_global_settings = global_settings;
    SCOPE_EXIT({ global_settings = old_global_settings; });
    // change the settings to make it more easy to trigger splitting segments
    Settings settings;
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_limit_size = 20;
    settings.dt_segment_delta_limit_rows = 7;
    settings.dt_segment_delta_limit_size = 20;
    settings.dt_segment_force_split_size = 100;
    settings.dt_segment_delta_cache_limit_size = 20;

    // we need change the settings in both the ctx we get just below and the global_context above.
    // because when processing write request, `DeltaMergeStore` will call `checkSegmentUpdate` with the context we just get below.
    // and when initialize `DeltaMergeStore`, it will call `checkSegmentUpdate` with the global_context above.
    // so we need to make the settings in these two contexts consistent.
    global_settings = settings;
    auto ctx = DMTestEnv::getContext(settings);
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    // create table
    auto create_table = [&]() {
        NamesAndTypesList names_and_types_list{
            {"col1", std::make_shared<DataTypeInt64>()},
            {"col2", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath("StorageDeltaMerge_RestoreAfterClearData");
        if (Poco::File path(path_name); path.exists())
            path.remove(true);

        // primary_expr_ast
        const String table_name = "t_1233";
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        // table_info.id is used as the ns_id
        TiDB::TableInfo table_info;
        table_info.id = 1233;
        table_info.is_common_handle = false;
        table_info.pk_is_handle = false;

        // max page id is only updated at restart, so we need recreate page v3 before recreate table
        ctx->getGlobalContext().initializeGlobalPageIdAllocator();
        ctx->getGlobalContext().initializeGlobalStoragePoolIfNeed(ctx->getPathPool());
        storage = StorageDeltaMerge::create(
            "TiFlash",
            /* db_name= */ "default",
            table_name,
            table_info,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            *ctx);
        storage->startup();
    };
    auto write_data = [&](Int64 start, Int64 limit) {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx->getSettingsRef());
        // prepare block data
        Block sample;
        sample.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(start, start + limit), "col1"));
        sample.insert(DB::tests::createColumn<String>(Strings(limit, "a"), "col2"));

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    };
    auto read_data = [&]() {
        auto scan_context = std::make_shared<DM::ScanContext>();
        QueryProcessingStage::Enum stage2;
        SelectQueryInfo query_info;
        query_info.query = std::make_shared<ASTSelectQuery>();
        query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(
            ctx->getSettingsRef().resolve_locks,
            std::numeric_limits<UInt64>::max(),
            scan_context);
        // these field should live long enough because `DAGQueryInfo` only
        // keep a ref on them
        const google::protobuf::RepeatedPtrField<tipb::Expr> filters{};
        const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
        TiDB::ColumnInfos source_columns{};
        const std::vector<int> runtime_filter_ids;
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            filters,
            pushed_down_filters, // Not care now
            source_columns, // Not care now
            runtime_filter_ids,
            0,
            ctx->getTimezoneInfo());
        Names read_columns = {"col1", EXTRA_TABLE_ID_COLUMN_NAME, "col2"};
        BlockInputStreams ins = storage->read(read_columns, query_info, *ctx, stage2, 8192, 1);
        return getInputStreamNRows(ins[0]);
    };

    // create table
    create_table();
    size_t num_rows_write = 0;
    // write until split and use a big enough finite for loop to make sure the test won't hang forever
    for (size_t i = 0; i < 100000; i++)
    {
        write_data(num_rows_write, 1000);
        num_rows_write += 1000;
        if (storage->getStore()->getSegmentsStats().size() > 1)
            break;
    }
    {
        ASSERT_GT(storage->getStore()->getSegmentsStats().size(), 1);
        ASSERT_EQ(read_data(), num_rows_write);
    }
    storage->flushCache(*ctx);
    // throw exception before drop first segment
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::exception_before_drop_segment);
    ASSERT_ANY_THROW(storage->clearData());
    storage->removeFromTMTContext();

    // restore the table and make sure no data has been dropped
    create_table();
    {
        ASSERT_EQ(read_data(), num_rows_write);
    }
    // write more data make sure segments more than 1
    for (size_t i = 0; i < 100000; i++)
    {
        if (storage->getStore()->getSegmentsStats().size() > 1)
            break;
        write_data(num_rows_write, 1000);
        num_rows_write += 1000;
    }
    {
        ASSERT_GT(storage->getStore()->getSegmentsStats().size(), 1);
        ASSERT_EQ(read_data(), num_rows_write);
    }
    storage->flushCache(*ctx);
    // throw exception after drop first segment
    DB::FailPointHelper::enableFailPoint(DB::FailPoints::exception_after_drop_segment);
    ASSERT_ANY_THROW(storage->clearData());
    storage->removeFromTMTContext();

    // restore the table and make sure some data has been dropped
    create_table();
    {
        ASSERT_LT(read_data(), num_rows_write);
    }
    storage->clearData();
    storage->removeFromTMTContext();

    // restore the table and make sure there is just one segment left
    create_table();
    {
        ASSERT_EQ(storage->getStore()->getSegmentsStats().size(), 1);
        ASSERT_LT(read_data(), num_rows_write);
    }
    storage->drop();
    // remove the storage from TiFlash context manually
    storage->removeFromTMTContext();
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB

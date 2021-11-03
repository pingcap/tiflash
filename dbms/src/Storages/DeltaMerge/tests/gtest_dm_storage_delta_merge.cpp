#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <TestUtils/FunctionTestUtils.h>

#include <limits>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{
TEST(StorageDeltaMerge_test, ReadWriteCase1)
try
{
    // prepare block data
    Block sample;
    sample.insert(DB::tests::createColumn<Int64>(
        createNumbers<Int64>(0, 100, /*reversed*/ true),
        "col1"));
    sample.insert(DB::tests::createColumn<String>(
        Strings(100, "a"),
        "col2"));

    Context ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
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

        const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath();
        Poco::File path(path_name);
        if (path.exists())
            path.remove(true);

        // primary_expr_ast
        const String table_name = "tmp_table";
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create("TiFlash",
                                            /* db_name= */ "default",
                                            table_name,
                                            std::nullopt,
                                            ColumnsDescription{names_and_types_list},
                                            astptr,
                                            0,
                                            ctx);
        storage->startup();
    }

    // test writing to DeltaMergeStorage
    {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx.getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(ctx.getSettingsRef().resolve_locks, std::numeric_limits<UInt64>::max());
    BlockInputStreams ins = storage->read(column_names, query_info, ctx, stage2, 8192, 1);
    ASSERT_EQ(ins.size(), 1UL);
    BlockInputStreamPtr in = ins[0];
    in->readPrefix();

    size_t num_rows_read = 0;
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    ASSERT_EQ(c->getInt(i), i);
                }
                else if (iter.name == "col2")
                {
                    ASSERT_EQ(c->getDataAt(i), "a");
                }
            }
        }
    }
    in->readSuffix();
    ASSERT_EQ(num_rows_read, sample.rows());

    auto store_status = storage->status();
    Block status = store_status->read();
    String name_str = "Name";
    String value_str = "Value";
    ColumnPtr col_name = status.getByName(name_str).column;
    ColumnPtr col_value = status.getByName(value_str).column;
    for (size_t i = 0; i < col_name->size(); i++)
    {
        if (col_name->getDataAt(i) == String("segment_count"))
        {
            EXPECT_EQ(col_value->getDataAt(i), String(DB::toString(1)));
        }
        else if (col_name->getDataAt(i) == String("total_rows"))
        {
            EXPECT_EQ(col_value->getDataAt(i), String(DB::toString(num_rows_read)));
        }
    }
    storage->drop();
}
CATCH

TEST(StorageDeltaMerge_test, Rename)
try
{
    Context ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath();
    const String table_name = "tmp_table";
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

        Poco::File path(path_name);
        if (path.exists())
        {
            path.remove(true);
        }

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create("TiFlash",
                                            db_name,
                                            table_name,
                                            std::nullopt,
                                            ColumnsDescription{names_and_types_list},
                                            astptr,
                                            0,
                                            ctx);
        storage->startup();
    }

    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getTableName(), table_name);
    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getDatabaseName(), db_name);
    ASSERT_FALSE(storage->storeInited());

    // Rename database name before store object is created.
    const String new_db_name = "new_" + storage->getDatabaseName();
    storage->rename(path_name, new_db_name, table_name, table_name);
    ASSERT_FALSE(storage->storeInited());
    ASSERT_EQ(storage->getTableName(), table_name);
    ASSERT_EQ(storage->getDatabaseName(), new_db_name);

    // prepare block data
    Block sample;
    sample.insert(DB::tests::createColumn<Int64>(
        createNumbers<Int64>(0, 100, /*reversed*/ true),
        "col1"));
    sample.insert(DB::tests::createColumn<String>(
        Strings(100, "a"),
        "col2"));
    // Writing will create store object.
    {
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx.getSettingsRef());
        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
        ASSERT_TRUE(storage->storeInited());
    }

    // Rename table name
    String new_table_name = "new_" + storage->getTableName();
    storage->rename(path_name, new_db_name, new_table_name, new_table_name);
    ASSERT_EQ(storage->getTableName(), new_table_name);
    ASSERT_EQ(storage->getDatabaseName(), new_db_name);
}
CATCH

TEST(StorageDeltaMerge_test, HandleCol)
try
{
    Context ctx = DMTestEnv::getContext();
    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath();
    const String table_name = "tmp_table";
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

        Poco::File path(path_name);
        if (path.exists())
        {
            path.remove(true);
        }

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create("TiFlash",
                                            db_name,
                                            table_name,
                                            std::nullopt,
                                            ColumnsDescription{names_and_types_list},
                                            astptr,
                                            0,
                                            ctx);
        storage->startup();
    }

    ASSERT_FALSE(storage->storeInited());
    auto pk_type = storage->getPKTypeImpl();
    auto sort_desc = storage->getPrimarySortDescription();
    ASSERT_FALSE(storage->storeInited());

    auto & store = storage->getStore();
    ASSERT_TRUE(storage->storeInited());
    auto pk_type2 = store->getPKDataType();
    auto sort_desc2 = store->getPrimarySortDescription();

    ASSERT_EQ(pk_type->getTypeId(), pk_type2->getTypeId());
    ASSERT_EQ(sort_desc.size(), 1u);
    ASSERT_EQ(sort_desc2.size(), 1u);
    ASSERT_EQ(sort_desc.front().column_name, sort_desc2.front().column_name);
    ASSERT_EQ(sort_desc.front().direction, sort_desc2.front().direction);
    ASSERT_EQ(sort_desc.front().nulls_direction, sort_desc2.front().nulls_direction);
}
CATCH

TEST(StorageDeltaMerge_internal_test, GetMergedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
    region.range_in_table = GET_REGION_RANGE(100, 200, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(200, 250, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(300, 400, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(425, 475, 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 3UL);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), ::DB::DM::HandleRange(100, 250));
    ASSERT_RANGE_EQ(ranges[1].toHandleRange(), ::DB::DM::HandleRange(300, 400));
    ASSERT_RANGE_EQ(ranges[2].toHandleRange(), ::DB::DM::HandleRange(425, 475));
}

TEST(StorageDeltaMerge_internal_test, GetMergedQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(200, 250, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2).toRegionRange(1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, true, 2);
    ASSERT_EQ(ranges.size(), 3UL);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(100, 250, 2));
    ASSERT_ROWKEY_RANGE_EQ(ranges[1], DMTestEnv::getRowKeyRangeForClusteredIndex(300, 400, 2));
    ASSERT_ROWKEY_RANGE_EQ(ranges[2], DMTestEnv::getRowKeyRangeForClusteredIndex(425, 475, 2));
}

TEST(StorageDeltaMerge_internal_test, MergedUnsortedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
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
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), DB::DM::HandleRange(1961680, 2950532));
}

TEST(StorageDeltaMerge_internal_test, MergedUnsortedQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
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
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(1961680, 2950532, 2));
}

TEST(StorageDeltaMerge_internal_test, GetFullQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
    region.range_in_table = GET_REGION_RANGE(std::numeric_limits<HandleID>::min(), std::numeric_limits<HandleID>::max(), 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 1UL);
    const auto full_range = ::DB::DM::HandleRange::newAll();
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), full_range);
}

TEST(StorageDeltaMerge_internal_test, OverlapQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
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

TEST(StorageDeltaMerge_internal_test, OverlapQueryRangesCommonHandle)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
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

TEST(StorageDeltaMerge_internal_test, WeirdRange)
{
    // [100, 200), [200, MAX]
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
    region.range_in_table = GET_REGION_RANGE(100, 200, 1);
    regions.emplace_back(region);
    region.range_in_table = GET_REGION_RANGE(200, std::numeric_limits<HandleID>::max(), 1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, false, 1);
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_RANGE_EQ(ranges[0].toHandleRange(), DB::DM::HandleRange(100, DB::DM::HandleRange::MAX));
}

TEST(StorageDeltaMerge_internal_test, WeirdRangeCommonHandle)
{
    // [100, 200), [200, MAX), [MAX, MAX)
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo region;
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(100, 200, 2).toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table
        = DMTestEnv::getRowKeyRangeForClusteredIndex(std::numeric_limits<HandleID>::max(), std::numeric_limits<HandleID>::max(), 2)
              .toRegionRange(1);
    regions.emplace_back(region);
    region.range_in_table = DMTestEnv::getRowKeyRangeForClusteredIndex(200, std::numeric_limits<HandleID>::max(), 2).toRegionRange(1);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions, 1, true, 2);
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_ROWKEY_RANGE_EQ(ranges[0], DMTestEnv::getRowKeyRangeForClusteredIndex(100, DB::DM::HandleRange::MAX, 2));
}

TEST(StorageDeltaMerge_internal_test, RangeSplit)
{
    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo region;
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
            ASSERT_EQ(ranges.size(), 2UL);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 1);
            ASSERT_EQ(ranges.size(), 2UL);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 2);
            ASSERT_EQ(ranges.size(), 2ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 3);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 4);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 5);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 100);
            ASSERT_EQ(ranges.size(), 4ul);
        }
    }

    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo region;
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
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 1);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 2);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 3);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 4);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 5);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1, true, 2, 100);
            ASSERT_EQ(ranges.size(), 4ul);
        }
    }
}


} // namespace tests
} // namespace DM
} // namespace DB

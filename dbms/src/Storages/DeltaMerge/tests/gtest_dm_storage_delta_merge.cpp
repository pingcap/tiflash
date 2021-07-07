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
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/TiKVRecordFormat.h>

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
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeInt64>();
        {
            IColumn::MutablePtr m_col = col1.type->createColumn();
            // insert form large to small
            for (int i = 0; i < 100; i++)
            {
                Field field = Int64(99 - i);
                m_col->insert(field);
            }
            col1.column = std::move(m_col);
        }
        sample.insert(col1);

        ColumnWithTypeAndName col2;
        col2.name = "col2";
        col2.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr m_col2 = col2.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field field("a", 1);
                m_col2->insert(field);
            }
            col2.column = std::move(m_col2);
        }
        sample.insert(col2);
    }

    Context    ctx = DMTestEnv::getContext();
    StoragePtr storage;
    DataTypes  data_types;
    Names      column_names;
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
        Poco::File   path(path_name);
        if (path.exists())
            path.remove(true);

        // primary_expr_ast
        const String table_name = "tmp_table";
        ASTPtr       astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
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
        ASTPtr               insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, ctx.getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo            query_info;
    query_info.query                          = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info                = std::make_unique<MvccQueryInfo>();
    query_info.mvcc_query_info->resolve_locks = ctx.getSettingsRef().resolve_locks;
    query_info.mvcc_query_info->read_tso      = std::numeric_limits<UInt64>::max();
    BlockInputStreams ins                     = storage->read(column_names, query_info, ctx, stage2, 8192, 1);
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


    storage->drop();
}
CATCH


TEST(StorageDeltaMerge_internal_test, GetMergedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = std::make_pair(100, 200);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(200, 250);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(300, 400);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(425, 475);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions);
    ASSERT_EQ(ranges.size(), 3UL);
    ASSERT_RANGE_EQ(ranges[0], ::DB::DM::HandleRange(100, 250));
    ASSERT_RANGE_EQ(ranges[1], ::DB::DM::HandleRange(300, 400));
    ASSERT_RANGE_EQ(ranges[2], ::DB::DM::HandleRange(425, 475));
}

TEST(StorageDeltaMerge_internal_test, MergedUnsortedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = std::make_pair(2360148, 2456148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(1961680, 2057680);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2264148, 2360148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2057680, 2153680);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2153680, 2264148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2552148, 2662532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2758532, 2854532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2854532, 2950532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2456148, 2552148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2662532, 2758532);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions);
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_RANGE_EQ(ranges[0], DB::DM::HandleRange(1961680, 2950532));
}

TEST(StorageDeltaMerge_internal_test, GetFullQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = {TiKVHandle::Handle<HandleID>::normal_min, TiKVHandle::Handle<HandleID>::max};
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions);
    ASSERT_EQ(ranges.size(), 1UL);
    const auto full_range = ::DB::DM::HandleRange::newAll();
    ASSERT_RANGE_EQ(ranges[0], full_range);
}

TEST(StorageDeltaMerge_internal_test, OverlapQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = std::make_pair(100, 200);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(150, 250);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(300, 400);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(425, 475);
    regions.emplace_back(region);

    // Overlaped ranges throw exception
    ASSERT_ANY_THROW(auto ranges = ::DB::getQueryRanges(regions));
}

TEST(StorageDeltaMerge_internal_test, WeirdRange)
{
    // [100, 200), [200, MAX)
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = DB::HandleRange<DB::HandleID>{100, 200};
    regions.emplace_back(region);
    region.range_in_table = DB::HandleRange<DB::HandleID>{200, TiKVRange::Handle::max};
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions);
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_RANGE_EQ(ranges[0], DB::DM::HandleRange(100, DB::DM::HandleRange::MAX));
}

TEST(StorageDeltaMerge_internal_test, RangeSplit)
{
    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo                 region;
        region.range_in_table = std::make_pair(100, 200);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(200, 300);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(300, 400);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(425, 475);
        regions.emplace_back(region);

        {
            auto ranges = DB::getQueryRanges(regions, 0);
            ASSERT_EQ(ranges.size(), 2UL);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1);
            ASSERT_EQ(ranges.size(), 2UL);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 2);
            ASSERT_EQ(ranges.size(), 2ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 3);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 4);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 5);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 100);
            ASSERT_EQ(ranges.size(), 4ul);
        }
    }

    {
        MvccQueryInfo::RegionsQueryInfo regions;
        RegionQueryInfo                 region;
        region.range_in_table = std::make_pair(0, 100);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(200, 300);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(300, 400);
        regions.emplace_back(region);
        region.range_in_table = std::make_pair(425, 475);
        regions.emplace_back(region);

        {
            auto ranges = DB::getQueryRanges(regions, 0);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 1);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 2);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 3);
            ASSERT_EQ(ranges.size(), 3ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 4);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 5);
            ASSERT_EQ(ranges.size(), 4ul);
        }

        {
            auto ranges = DB::getQueryRanges(regions, 100);
            ASSERT_EQ(ranges.size(), 4ul);
        }
    }
}


} // namespace tests
} // namespace DM
} // namespace DB

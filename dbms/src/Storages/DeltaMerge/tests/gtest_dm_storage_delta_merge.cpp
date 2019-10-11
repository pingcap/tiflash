#include <gtest/gtest.h>
#include "dm_basic_include.h"

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
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
#include <Storages/DeltaMerge/DummyDeltaMergeBlockInputStream.h>
#include <Storages/DeltaMerge/DummyDeltaMergeBlockOutputStream.h>
#include <Storages/StorageDeltaMerge-internal.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeDummy.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{
namespace DM
{
namespace tests
{


TEST(StorageDeltaMergeDummy_test, ReadWriteCase1)
{
    // prepare block data
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        {
            IColumn::MutablePtr m_col = col1.type->createColumn();
            // insert form large to small
            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(99 - i);
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

    StoragePtr storage;
    DataTypes  data_types;
    Names      column_names;
    // create table
    {
        NamesAndTypesList names_and_types_list{
            {"col1", std::make_shared<DataTypeUInt64>()},
            {"col2", std::make_shared<DataTypeString>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        ASTPtr astptr(new ASTIdentifier("mytemptable", ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMergeDummy::create(".", "mytemptable", ColumnsDescription{names_and_types_list}, astptr, false, 1000);
        storage->startup();
    }

    // test writing to DeltaMergeStorage
    {
        ASTPtr               insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, DMTestEnv::getContext().getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        dms = storage->read(column_names, {}, DMTestEnv::getContext(), stage2, 8192, 1)[0];

    dms->readPrefix();

    size_t num_rows_read = 0;
    while (Block block = dms->read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    ASSERT_EQ(c->getUInt(i), i);
                }
                else if (iter.name == "col2")
                {
                    ASSERT_EQ(c->getDataAt(i), "a");
                }
            }
        }
    }
    dms->readSuffix();
    ASSERT_EQ(num_rows_read, sample.rows());
}

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

    StoragePtr storage;
    DataTypes  data_types;
    Names      column_names;
    // create table
    {
        // FIXME: if we define col1 as UInt64, exception raised while reading data
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

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier("t", ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        storage = StorageDeltaMerge::create(".",
                                            /* db_name= */ "default",
                                            /* name= */ "t",
                                            std::nullopt,
                                            ColumnsDescription{names_and_types_list},
                                            astptr,
                                            DMTestEnv::getContext());
        storage->startup();
    }

    // test writing to DeltaMergeStorage
    {
        ASTPtr               insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, DMTestEnv::getContext().getSettingsRef());

        output->writePrefix();
        output->write(sample);
        output->writeSuffix();
    }

    // get read stream from DeltaMergeStorage
    Context &                  global_ctx = DMTestEnv::getContext();
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo            query_info;
    query_info.query                          = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info                = std::make_unique<MvccQueryInfo>();
    query_info.mvcc_query_info->resolve_locks = global_ctx.getSettingsRef().resolve_locks;
    query_info.mvcc_query_info->read_tso      = global_ctx.getSettingsRef().read_tso;
    BlockInputStreams ins                     = storage->read(column_names, query_info, global_ctx, stage2, 8192, 1);
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
catch (const Exception & e)
{
    const auto text = e.displayText();
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString();

    throw;
}


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
    ASSERT_EQ(ranges[0], ::DB::DM::HandleRange(100, 250));
    ASSERT_EQ(ranges[1], ::DB::DM::HandleRange(300, 400));
    ASSERT_EQ(ranges[2], ::DB::DM::HandleRange(425, 475));
}

TEST(StorageDeltaMerge_internal_test, MergedUnsortedQueryRanges)
{
    MvccQueryInfo::RegionsQueryInfo regions;
    RegionQueryInfo                 region;
    region.range_in_table = std::make_pair(2360148,2456148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(1961680,2057680);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2264148,2360148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2057680,2153680);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2153680,2264148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2552148,2662532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2758532,2854532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2854532,2950532);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2456148,2552148);
    regions.emplace_back(region);
    region.range_in_table = std::make_pair(2662532,2758532);
    regions.emplace_back(region);

    auto ranges = ::DB::getQueryRanges(regions);
    ASSERT_EQ(ranges.size(), 1UL);
    ASSERT_EQ(ranges[0], ::DB::DM::HandleRange(1961680, 2950532)) << ranges[0].toString();
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
    ASSERT_EQ(ranges[0], full_range);
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

    ASSERT_ANY_THROW(auto ranges = ::DB::getQueryRanges(regions));
}

} // namespace tests
} // namespace DM
} // namespace DB

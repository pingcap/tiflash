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
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeDummy.h>

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

        Context context = DMTestEnv::getContext();
        storage         = StorageDeltaMerge::create(".", "t", ColumnsDescription{names_and_types_list}, astptr, context);
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
    SelectQueryInfo            query_info;
    query_info.query        = std::make_shared<ASTSelectQuery>();
    BlockInputStreamPtr dms = storage->read(column_names, query_info, DMTestEnv::getContext(), stage2, 8192, 1)[0];
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
                    ASSERT_EQ(c->getInt(i), i);
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

} // namespace tests
} // namespace DM
} // namespace DB

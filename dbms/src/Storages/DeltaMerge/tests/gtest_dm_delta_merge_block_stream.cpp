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
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DummyDeltaMergeBlockInputStream.h>
#include <Storages/DeltaMerge/DummyDeltaMergeBlockOutputStream.h>
#include <Storages/StorageMemory.h>


namespace DB
{
namespace DM
{
namespace tests
{

using namespace Dummy;

class DummyDeltaMergeBlockInputStream_test : public ::testing::Test
{
public:
    DummyDeltaMergeBlockInputStream_test() = default;

protected:
};

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaTreeInsert)
{
    // prepare data to store in stable. 0,2,4,6....
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        {
            IColumn::MutablePtr tempcol = col1.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(i * 2);
                tempcol->insert(field);
            }
            col1.column = std::move(tempcol);
        }
        sample.insert(col1);
    }

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
    };

    DataTypes data_types;
    Names     column_names;
    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create a table in memory
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    // write col1 to table
    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());
    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    // prepare data to insert. 1,3,5,7...
    Block sample2;
    {
        ColumnWithTypeAndName col2;
        col2.name = "col1";
        col2.type = std::make_shared<DataTypeUInt64>();
        {
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(i * 2 + 1);
                tempcol2->insert(field);
            }
            col2.column = std::move(tempcol2);
        }
        sample2.insert(col2);
    }
    // insert data to delta_tree
    Ids id_vec = insert_value_space->addFromInsert(sample2);
    for (unsigned int i = 0; i < id_vec.size(); i++)
    {
        delta_tree->addInsert(2 * i + 1, id_vec[i]);
    }

    // get merged stream from both stable & delta_tree
    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                num_rows_read += 1;
                EXPECT_EQ(c->getUInt(i), i);
            }
        }
    }
    dms.readSuffix();
    // check the merged stream length
    ASSERT_EQ(num_rows_read, sample.rows() + sample2.rows());
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaTreeUpdate)
{
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name = "col1";
    col1.type = std::make_shared<DataTypeUInt64>();
    {
        IColumn::MutablePtr tempcol = col1.type->createColumn();
        for (int i = 0; i < 100; i++)
        {
            Field field = UInt64(i * 2 + 1);
            tempcol->insert(field);
        }
        col1.column = std::move(tempcol);
    }
    sample.insert(col1);

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
    };
    DataTypes data_types;
    Names     column_names;
    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create table
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());
    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    // prepare data for update
    Block sample2;
    {
        ColumnWithTypeAndName col2;
        col2.name = "col1";
        col2.type = std::make_shared<DataTypeUInt64>();
        {
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(i * 2 + 2);
                tempcol2->insert(field);
            }
            col2.column = std::move(tempcol2);
        }
        sample2.insert(col2);
    }
    // update data in delta_tree
    RefTuples rts = insert_value_space->addFromModify(sample2);
    for (unsigned int i = 0; i < rts.size(); i++)
    {
        delta_tree->addModify(i, rts[i]);
    }

    // get merged stream from both stable & delta_tree
    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                num_rows_read += 1;
                // printf("%llu\n", c->getUInt(i));
                // FIXME the first value did't get updated
                EXPECT_EQ(c->getUInt(i), i * 2 + 2);
            }
        }
    }
    dms.readSuffix();
    // check the merged stream length
    ASSERT_EQ(num_rows_read, sample.rows());
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaTreeDelete)
{
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                   = "col1";
    col1.type                   = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr tempcol = col1.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(i);
        tempcol->insert(field);
    }

    col1.column = std::move(tempcol);
    sample.insert(col1);

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create table
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();
    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());
    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    const size_t num_rows_del = 50;
    for (unsigned int i = 0; i < num_rows_del; i++)
    {
        delta_tree->addDelete(0);
    }

    // get merged stream from both stable & delta_tree
    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                num_rows_read += 1;
                EXPECT_EQ(c->getUInt(i), i + 50);
            }
        }
    }
    dms.readSuffix();
    // check the merged stream length
    ASSERT_EQ(num_rows_read, sample.rows() - num_rows_del);
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaOutputInsert)
{
    // block write into stable storage
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2;
        col2.name = "col2";
        col2.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol  = col1.type->createColumn();
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field ifield = UInt64(2 * i);
                tempcol->insert(ifield);
                Field afield("a", 1);
                tempcol2->insert(afield);
            }
            col1.column = std::move(tempcol);
            col2.column = std::move(tempcol2);
        }
        sample.insert(col1);
        sample.insert(col2);
    }

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
        {"col2", std::make_shared<DataTypeString>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create a table in memory
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());
    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DummyDeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, DMTestEnv::getContext(), stage2, 8192, 1)[0];

    BlockOutputStreamPtr delta_output_stream = std::make_shared<DummyDeltaMergeBlockOutputStream>(
        input_stream_creator, delta_tree, Action::Insert, sd, []() {}, 10000);

    // data write through DummyDeltaMergeBlockOutputStream
    Block sample_delta;
    {
        ColumnWithTypeAndName col1_delta;
        col1_delta.name = "col1";
        col1_delta.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2_delta;
        col2_delta.name = "col2";
        col2_delta.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol_delta  = col1_delta.type->createColumn();
            IColumn::MutablePtr tempcol2_delta = col2_delta.type->createColumn();
            for (int i = 0; i < 100; i++)
            {
                Field ifield = UInt64(2 * i + 1);
                tempcol_delta->insert(ifield);
                Field afield("a", 1);
                tempcol2_delta->insert(afield);
            }
            col1_delta.column = std::move(tempcol_delta);
            col2_delta.column = std::move(tempcol2_delta);
        }
        sample_delta.insert(col1_delta);
        sample_delta.insert(col2_delta);
    }
    delta_output_stream->write(sample_delta);

    // get merged stream
    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    EXPECT_EQ(c->getUInt(i), i);
                }
                else if (iter.name == "col2")
                {
                    EXPECT_EQ(c->getDataAt(i), "a");
                }
            }
        }
    }
    dms.readSuffix();
    ASSERT_EQ(num_rows_read, sample.rows() + sample_delta.rows())
        << "sample rows:" << sample.rows() << " sample_delta.rows:" << sample_delta.rows();
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaOutputUpsert)
{
    // block write into stable storage
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2;
        col2.name = "col2";
        col2.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol  = col1.type->createColumn();
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol->insert(field);
                Field afield("a", 1);
                tempcol2->insert(afield);
            }

            col1.column = std::move(tempcol);
            col2.column = std::move(tempcol2);
        }
        sample.insert(col1);
        sample.insert(col2);
    }

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
        {"col2", std::make_shared<DataTypeString>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create table
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DummyDeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream = std::make_shared<DummyDeltaMergeBlockOutputStream>(
        input_stream_creator, delta_tree, Action::Upsert, sd, []() {}, 10000);

    // data write through DummyDeltaMergeBlockOutputStream
    Block sample_delta;
    {
        ColumnWithTypeAndName col1_delta;
        col1_delta.name = "col1";
        col1_delta.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2_delta;
        col2_delta.name = "col2";
        col2_delta.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol_delta  = col1_delta.type->createColumn();
            IColumn::MutablePtr tempcol2_delta = col2_delta.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol_delta->insert(field);
                Field afield("b", 1);
                tempcol2_delta->insert(afield);
            }

            col1_delta.column = std::move(tempcol_delta);
            col2_delta.column = std::move(tempcol2_delta);
        }
        sample_delta.insert(col1_delta);
        sample_delta.insert(col2_delta);
    }

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum      stage2;
    BlockInputStreamPtr             in = storage->read(column_names, {}, DMTestEnv::getContext(), stage2, 8192, 1)[0];
    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    ASSERT_EQ(c->getUInt(i), 2 * i);
                }
                else if (iter.name == "col2")
                {
                    ASSERT_EQ(c->getDataAt(i), "b");
                }
            }
        }
    }
    dms.readSuffix();
    // check num rows
    ASSERT_EQ(num_rows_read, sample.rows());
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaOutputDelete)
{
    // block write into stable storage
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2;
        col2.name = "col2";
        col2.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol  = col1.type->createColumn();
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol->insert(field);
                Field afield("a", 1);
                tempcol2->insert(afield);
            }

            col1.column = std::move(tempcol);
            col2.column = std::move(tempcol2);
        }
        sample.insert(col1);
        sample.insert(col2);
    }

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
        {"col2", std::make_shared<DataTypeString>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create table
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DummyDeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream = std::make_shared<DummyDeltaMergeBlockOutputStream>(
        input_stream_creator, delta_tree, Action::Delete, sd, []() {}, 10000);

    // data write through DummyDeltaMergeBlockOutputStream
    Block sample_delta;
    {
        ColumnWithTypeAndName col1_delta;
        col1_delta.name = "col1";
        col1_delta.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2_delta;
        col2_delta.name = "col2";
        col2_delta.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol_delta  = col1_delta.type->createColumn();
            IColumn::MutablePtr tempcol2_delta = col2_delta.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol_delta->insert(field);
                Field afield("a", 1);
                tempcol2_delta->insert(afield);
            }

            col1_delta.column = std::move(tempcol_delta);
            col2_delta.column = std::move(tempcol2_delta);
        }
        sample_delta.insert(col1_delta);
        sample_delta.insert(col2_delta);
    }

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, DMTestEnv::getContext(), stage2, 8192, 1)[0];

    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    EXPECT_EQ(c->getUInt(i), i);
                }
                else if (iter.name == "col2")
                {
                    EXPECT_EQ(c->getDataAt(i), "a");
                }
            }
        }
    }
    dms.readSuffix();
    // nothing since we delete all data
    ASSERT_EQ(num_rows_read, 0UL);
}

TEST_F(DummyDeltaMergeBlockInputStream_test, DeltaOutputUpdate)
{
    // block write into stable storage
    Block sample;
    {
        ColumnWithTypeAndName col1;
        col1.name = "col1";
        col1.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2;
        col2.name = "col2";
        col2.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol  = col1.type->createColumn();
            IColumn::MutablePtr tempcol2 = col2.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol->insert(field);
                Field afield("a", 1);
                tempcol2->insert(afield);
            }

            col1.column = std::move(tempcol);
            col2.column = std::move(tempcol2);
        }
        sample.insert(col1);
        sample.insert(col2);
    }

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()},
        {"col2", std::make_shared<DataTypeString>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    // create table
    StoragePtr storage = StorageMemory::create("mytemptable", ColumnsDescription{names_and_types_list});
    storage->startup();

    BlockOutputStreamPtr output = storage->write(ASTPtr(), DMTestEnv::getContext().getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DummyDeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, DMTestEnv::getContext(), stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<MyDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream = std::make_shared<DummyDeltaMergeBlockOutputStream>(
        input_stream_creator, delta_tree, Action::Update, sd, []() {}, 10000);

    // data write through DummyDeltaMergeBlockOutputStream
    Block sample_delta;
    {
        ColumnWithTypeAndName col1_delta;
        col1_delta.name = "col1";
        col1_delta.type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col2_delta;
        col2_delta.name = "col2";
        col2_delta.type = std::make_shared<DataTypeString>();
        {
            IColumn::MutablePtr tempcol_delta  = col1_delta.type->createColumn();
            IColumn::MutablePtr tempcol2_delta = col2_delta.type->createColumn();

            for (int i = 0; i < 100; i++)
            {
                Field field = UInt64(2 * i);
                tempcol_delta->insert(field);
                Field afield("b", 1);
                tempcol2_delta->insert(afield);
            }

            col1_delta.column = std::move(tempcol_delta);
            col2_delta.column = std::move(tempcol2_delta);
        }
        sample_delta.insert(col1_delta);
        sample_delta.insert(col2_delta);
    }
    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, DMTestEnv::getContext(), stage2, 8192, 1)[0];

    DummyDeltaMergeBlockInputStream dms(in, delta_tree, 8192);
    dms.readPrefix();
    size_t num_rows_read = 0;
    while (Block block = dms.read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter.name == "col1")
                {
                    EXPECT_EQ(c->getUInt(i), 2 * i);
                }
                else if (iter.name == "col2")
                {
                    EXPECT_EQ(c->getDataAt(i), "b");
                }
            }
        }
    }
    dms.readSuffix();
    // check rows num
    ASSERT_EQ(num_rows_read, sample.rows());
}

} // namespace tests
} // namespace DM
} // namespace DB

#include <iostream>
#include <string>

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
#include <Storages/DeltaMerge/DummyDeltaMergeBlockInputStream.h>
#include <Storages/DeltaMerge/DummyDeltaMergeBlockOutputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/StorageMemory.h>

using namespace DB;

//using MyValueSpacePtr = std::shared_ptr<MemoryValueSpace>;
//using MyDeltaTree     = DeltaTree<MemoryValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
//using MyDeltaTreePtr  = std::shared_ptr<MyDeltaTree>;

std::string treeToString(MyDeltaTreePtr tree)
{
    std::string result = "";
    std::string temp;
    for (auto it = tree->begin(), end = tree->end(); it != end; ++it)
    {
        temp = "";
        temp += "(";
        temp += std::to_string(it.getRid());
        temp += "|";
        temp += std::to_string(it.getSid());
        temp += "|";
        temp += DTTypeString(it.getMutation().type);
        temp += "|";
        temp += DB::toString(it.getMutation().value);
        temp += "),";
        result += temp;
    }
    return result;
}

void deltaTreeInsertTest(Context context)
{
    Block sample;

    ColumnWithTypeAndName col1;
    col1.name                   = "col1";
    col1.type                   = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr tempcol = col1.type->createColumn();


    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(i * 2);
        tempcol->insert(field);
    }

    col1.column = std::move(tempcol);

    sample.insert(col1);

    Block sample2;

    ColumnWithTypeAndName col2;
    col2.name                    = "col1";
    col2.type                    = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();


    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(i * 2 + 1);
        tempcol2->insert(field);
    }

    col2.column = std::move(tempcol2);

    sample2.insert(col2);


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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();

    output->write(sample);

    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, context, stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    Ids id_vec = insert_value_space->addFromInsert(sample2);
    for (unsigned int i = 0; i < id_vec.size(); i++)
    {
        delta_tree->addInsert(2 * i + 1, id_vec[i]);
    }

    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                assert(c->getUInt(i) == i);
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeUpdateTest(Context context)
{
    Block sample;

    ColumnWithTypeAndName col1;
    col1.name                   = "col1";
    col1.type                   = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr tempcol = col1.type->createColumn();


    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(i * 2);
        tempcol->insert(field);
    }

    col1.column = std::move(tempcol);

    sample.insert(col1);

    Block sample2;

    ColumnWithTypeAndName col2;
    col2.name                    = "col1";
    col2.type                    = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();


    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(i * 2 + 1);
        tempcol2->insert(field);
    }

    col2.column = std::move(tempcol2);

    sample2.insert(col2);


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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();

    output->write(sample);

    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, context, stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    RefTuples rts = insert_value_space->addFromModify(sample2);
    for (unsigned int i = 0; i < rts.size(); i++)
    {
        delta_tree->addModify(i, rts[i]);
    }

    //std::cout << treeToString(delta_tree) << std::endl;

    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                //std::cout << c->getUInt(i) << std::endl;
                assert(c->getUInt(i) == i * 2 + 1);
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeDeleteTest(Context context)
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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();

    output->write(sample);

    output->writeSuffix();

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = storage->read(column_names, {}, context, stage, 8192, 1)[0];

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, SortDescription{});
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, SortDescription{});
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    for (unsigned int i = 0; i < 50; i++)
    {
        delta_tree->addDelete(0);
    }

    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                //std::cout << i+50 << std::endl;
                assert(c->getUInt(i) == i + 50);
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeOutputInsertTest(Context context)
{
    // block write into stable storage
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                     = "col1";
    col1.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol = col1.type->createColumn();
    ColumnWithTypeAndName col2;
    col2.name                    = "col2";
    col2.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2->insert(field);
    }

    col1.column = std::move(tempcol);
    col2.column = std::move(tempcol2);
    sample.insert(col1);
    sample.insert(col2);

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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names, context]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, context, stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream
        = std::make_shared<DeltaMergeBlockOutputStream>(input_stream_creator, delta_tree, Action::Insert, sd, []() {}, 10000);

    // data write through DeltaMergeBlockOutputStream
    Block                 sample_delta;
    ColumnWithTypeAndName col1_delta;
    col1_delta.name                     = "col1";
    col1_delta.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol_delta = col1_delta.type->createColumn();
    ColumnWithTypeAndName col2_delta;
    col2_delta.name                    = "col2";
    col2_delta.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2_delta = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i + 1);
        tempcol_delta->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2_delta->insert(field);
    }
    col1_delta.column = std::move(tempcol_delta);
    col2_delta.column = std::move(tempcol2_delta);
    sample_delta.insert(col1_delta);
    sample_delta.insert(col2_delta);

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, context, stage2, 8192, 1)[0];
    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter->name == "col1")
                {
                    //std::cout << c->getUInt(i) << std::endl;
                    assert(c->getUInt(i) == i);
                }
                else if (iter->name == "col2")
                {
                    //std::cout << c->getDataAt(i) << std::endl;
                    assert(c->getDataAt(i) == "a");
                }
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeOutputUpsertTest(Context context)
{
    // block write into stable storage
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                     = "col1";
    col1.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol = col1.type->createColumn();
    ColumnWithTypeAndName col2;
    col2.name                    = "col2";
    col2.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2->insert(field);
    }

    col1.column = std::move(tempcol);
    col2.column = std::move(tempcol2);
    sample.insert(col1);
    sample.insert(col2);

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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names, context]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, context, stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream
        = std::make_shared<DeltaMergeBlockOutputStream>(input_stream_creator, delta_tree, Action::Upsert, sd, []() {}, 10000);

    // data write through DeltaMergeBlockOutputStream
    Block                 sample_delta;
    ColumnWithTypeAndName col1_delta;
    col1_delta.name                     = "col1";
    col1_delta.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol_delta = col1_delta.type->createColumn();
    ColumnWithTypeAndName col2_delta;
    col2_delta.name                    = "col2";
    col2_delta.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2_delta = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol_delta->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("b", 1);
        tempcol2_delta->insert(field);
    }
    col1_delta.column = std::move(tempcol_delta);
    col2_delta.column = std::move(tempcol2_delta);
    sample_delta.insert(col1_delta);
    sample_delta.insert(col2_delta);

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, context, stage2, 8192, 1)[0];
    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter->name == "col1")
                {
                    //std::cout << c->getUInt(i) << std::endl;
                    assert(c->getUInt(i) == 2 * i);
                }
                else if (iter->name == "col2")
                {
                    //std::cout << c->getDataAt(i) << std::endl;
                    assert(c->getDataAt(i) == "b");
                }
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeOutputDeleteTest(Context context)
{
    // block write into stable storage
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                     = "col1";
    col1.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol = col1.type->createColumn();
    ColumnWithTypeAndName col2;
    col2.name                    = "col2";
    col2.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2->insert(field);
    }

    col1.column = std::move(tempcol);
    col2.column = std::move(tempcol2);
    sample.insert(col1);
    sample.insert(col2);

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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names, context]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, context, stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream
        = std::make_shared<DeltaMergeBlockOutputStream>(input_stream_creator, delta_tree, Action::Delete, sd, []() {}, 10000);

    // data write through DeltaMergeBlockOutputStream
    Block                 sample_delta;
    ColumnWithTypeAndName col1_delta;
    col1_delta.name                     = "col1";
    col1_delta.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol_delta = col1_delta.type->createColumn();
    ColumnWithTypeAndName col2_delta;
    col2_delta.name                    = "col2";
    col2_delta.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2_delta = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol_delta->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2_delta->insert(field);
    }
    col1_delta.column = std::move(tempcol_delta);
    col2_delta.column = std::move(tempcol2_delta);
    sample_delta.insert(col1_delta);
    sample_delta.insert(col2_delta);

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, context, stage2, 8192, 1)[0];
    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter->name == "col1")
                {
                    std::cout << c->getUInt(i) << std::endl;
                    //assert(c->getUInt(i) == i);
                }
                else if (iter->name == "col2")
                {
                    std::cout << c->getDataAt(i) << std::endl;
                    //assert(c->getDataAt(i) == "a");
                }
            }
        }
    }

    dms.readSuffix();
}

void deltaTreeOutputUpdateTest(Context context)
{
    // block write into stable storage
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                     = "col1";
    col1.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol = col1.type->createColumn();
    ColumnWithTypeAndName col2;
    col2.name                    = "col2";
    col2.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2 = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        tempcol2->insert(field);
    }

    col1.column = std::move(tempcol);
    col2.column = std::move(tempcol2);
    sample.insert(col1);
    sample.insert(col2);

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

    BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    DeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [storage, column_names, context]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return storage->read(column_names, {}, context, stage, 8192, 1)[0];
    };

    SortDescription sd{};
    sd.emplace_back("col1", 1, 1);

    MyValueSpacePtr insert_value_space = std::make_shared<MemoryValueSpace>("insert_value_space", names_and_types_list, sd);
    MyValueSpacePtr modify_value_space = std::make_shared<MemoryValueSpace>("modify_value_space", names_and_types_list, sd);
    MyDeltaTreePtr  delta_tree         = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    BlockOutputStreamPtr delta_output_stream
        = std::make_shared<DeltaMergeBlockOutputStream>(input_stream_creator, delta_tree, Action::Update, sd, []() {}, 10000);

    // data write through DeltaMergeBlockOutputStream
    Block                 sample_delta;
    ColumnWithTypeAndName col1_delta;
    col1_delta.name                     = "col1";
    col1_delta.type                     = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   tempcol_delta = col1_delta.type->createColumn();
    ColumnWithTypeAndName col2_delta;
    col2_delta.name                    = "col2";
    col2_delta.type                    = std::make_shared<DataTypeString>();
    IColumn::MutablePtr tempcol2_delta = col2.type->createColumn();

    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(2 * i);
        tempcol_delta->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("b", 1);
        tempcol2_delta->insert(field);
    }
    col1_delta.column = std::move(tempcol_delta);
    col2_delta.column = std::move(tempcol2_delta);
    sample_delta.insert(col1_delta);
    sample_delta.insert(col2_delta);

    delta_output_stream->write(sample_delta);

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr        in = storage->read(column_names, {}, context, stage2, 8192, 1)[0];
    DeltaMergeBlockInputStream dms(in, delta_tree, 8192);

    dms.readPrefix();

    while (Block block = dms.read())
    {
        for (auto iter = block.begin(); iter != block.end(); iter++)
        {
            auto c = iter->column;
            for (unsigned int i = 0; i < c->size(); i++)
            {
                if (iter->name == "col1")
                {
                    std::cout << c->getUInt(i) << std::endl;
                    assert(c->getUInt(i) == 2 * i);
                }
                else if (iter->name == "col2")
                {
                    std::cout << c->getDataAt(i) << std::endl;
                    //assert(c->getDataAt(i) == "b");
                }
            }
        }
    }

    dms.readSuffix();
}

int main(int, char **) try
{
    Context context = Context::createGlobal();
    deltaTreeInsertTest(context);
    deltaTreeUpdateTest(context);
    deltaTreeDeleteTest(context);
    deltaTreeOutputInsertTest(context);
    std::cout << "after deltaTreeOutputInsertTest test\n";
    deltaTreeOutputUpsertTest(context);
    std::cout << "after deltaTreeOutputUpsertTest test\n";
    deltaTreeOutputDeleteTest(context);
    std::cout << "after deltaTreeOutputDeleteTest test\n";
    deltaTreeOutputUpdateTest(context);
    std::cout << "after deltaTreeOutputUpdateTest test\n";
    std::cout << "test complete\n";
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}

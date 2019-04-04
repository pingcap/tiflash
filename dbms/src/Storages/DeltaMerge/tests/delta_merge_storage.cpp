#include <string>
#include <iostream>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/copyData.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/DeltaMergeBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Core/SortDescription.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/DeltaMerge/DeltaMergeBlockOutputStream.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/StorageDeltaMerge.h>

using namespace DB;

int main(int, char **) try
{
    // block write into stable storage
    Block                 sample;
    ColumnWithTypeAndName col1;
    col1.name                   = "col1";
    col1.type                   = std::make_shared<DataTypeUInt64>();
    IColumn::MutablePtr   m_col = col1.type->createColumn();
    ColumnWithTypeAndName col2;
    col2.name                  = "col2";
    col2.type                  = std::make_shared<DataTypeString>();
    IColumn::MutablePtr m_col2 = col2.type->createColumn();

    // insert form large to small
    for (int i = 0; i < 100; i++)
    {
        Field field = UInt64(99 - i);
        m_col->insert(field);
    }

    for (int i = 0; i < 100; i++)
    {
        Field field("a", 1);
        m_col2->insert(field);
    }

    col1.column = std::move(m_col);
    col2.column = std::move(m_col2);
    sample.insert(col1);
    sample.insert(col2);

    NamesAndTypesList names_and_types_list{
        {"col1", std::make_shared<DataTypeUInt64>()}, {"col2", std::make_shared<DataTypeString>()},
    };

    DataTypes data_types;
    Names     column_names;

    for (const auto & name_type : names_and_types_list)
    {
        data_types.push_back(name_type.type);
        column_names.push_back(name_type.name);
    }

    ASTPtr astptr(new ASTIdentifier("mytemptable", ASTIdentifier::Kind::Table));
    astptr->children.emplace_back(new ASTIdentifier("col1"));

    ASTPtr insertptr(new ASTInsertQuery());

    Context context = Context::createGlobal();

    StoragePtr storage = StorageDeltaMerge::create(".", "mytemptable", ColumnsDescription{names_and_types_list}, astptr, false, 1000);
    storage->startup();
    BlockOutputStreamPtr output = storage->write(insertptr, context.getSettingsRef());

    output->writePrefix();
    output->write(sample);
    output->writeSuffix();

    QueryProcessingStage::Enum stage2;
    BlockInputStreamPtr dms = storage->read(column_names, {}, context, stage2, 8192, 1)[0];

    dms->readPrefix();

    while (Block block = dms->read())
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
                    //assert(c->getDataAt(i) == "b");
                }
            }
        }
    }

    dms->readSuffix();

    std::cout << "test complete\n";
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}

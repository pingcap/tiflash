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

#include <Common/typeid_cast.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/MutableSupport.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
BlockIO InterpreterDescribeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterDescribeQuery::getSampleBlock()
{
    Block block;

    ColumnWithTypeAndName col;
    col.name = "name";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    block.insert(col);

    col.name = "type";
    block.insert(col);

    col.name = "default_type";
    block.insert(col);

    col.name = "default_expression";
    block.insert(col);

    return block;
}


BlockInputStreamPtr InterpreterDescribeQuery::executeImpl()
{
    const ASTDescribeQuery & ast = typeid_cast<const ASTDescribeQuery &>(*query_ptr);

    NamesAndTypesList columns;
    ColumnDefaults column_defaults;
    StoragePtr table;

    auto table_expression = typeid_cast<const ASTTableExpression *>(ast.table_expression.get());

    if (table_expression->subquery)
    {
        columns = InterpreterSelectWithUnionQuery::getSampleBlock(table_expression->subquery->children[0], context)
                      .getNamesAndTypesList();
    }
    else
    {
        if (table_expression->table_function)
        {
            auto table_function = typeid_cast<const ASTFunction *>(table_expression->table_function.get());
            /// Get the table function
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_function->name, context);
            /// Run it and remember the result
            table = table_function_ptr->execute(table_expression->table_function, context);
        }
        else
        {
            String database_name;
            String table_name;

            auto identifier = table_expression->database_and_table_name;
            if (identifier->children.size() > 2)
                throw Exception(
                    "Logical error: more than two components in table expression",
                    ErrorCodes::LOGICAL_ERROR);

            if (identifier->children.size() > 1)
            {
                auto database_ptr = identifier->children[0];
                auto table_ptr = identifier->children[1];

                if (database_ptr)
                    database_name = typeid_cast<ASTIdentifier &>(*database_ptr).name;
                if (table_ptr)
                    table_name = typeid_cast<ASTIdentifier &>(*table_ptr).name;
            }
            else
                table_name = typeid_cast<ASTIdentifier &>(*identifier).name;

            table = context.getTable(database_name, table_name);
        }

        auto table_lock = table->lockStructureForShare(context.getCurrentQueryId());
        columns = table->getColumns().getAll();
        column_defaults = table->getColumns().defaults;
    }

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    const OrderedNameSet filtered_names = MutableSupport::instance().hiddenColumns(table->getName());

    for (const auto & column : columns)
    {
        if (filtered_names.has(column.name))
            continue;

        res_columns[0]->insert(column.name);
        res_columns[1]->insert(column.type->getName());

        const auto it = column_defaults.find(column.name);
        if (it == std::end(column_defaults))
        {
            res_columns[2]->insertDefault();
            res_columns[3]->insertDefault();
        }
        else
        {
            res_columns[2]->insert(toString(it->second.kind));
            res_columns[3]->insert(queryToString(it->second.expression));
        }
    }

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

} // namespace DB

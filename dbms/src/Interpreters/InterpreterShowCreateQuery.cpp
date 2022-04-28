// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int THERE_IS_NO_QUERY;
} // namespace ErrorCodes

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{ColumnString::create(),
                  std::make_shared<DataTypeString>(),
                  "statement"}};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    const auto & ast = dynamic_cast<const ASTQueryWithTableAndOutput &>(*query_ptr);

    if (ast.temporary && !ast.database.empty())
        throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);

    ASTPtr create_query;
    if (ast.temporary)
        create_query = context.getCreateExternalTableQuery(ast.table);
    else if (ast.table.empty())
        create_query = context.getCreateDatabaseQuery(ast.database);
    else
        create_query = context.getCreateTableQuery(ast.database, ast.table);

    if (!create_query && ast.temporary)
        throw Exception("Unable to show the create query of " + ast.table + ". Maybe it was created by the system.", ErrorCodes::THERE_IS_NO_QUERY);

    std::stringstream stream;
    formatAST(*create_query, stream, false, true);
    String res = stream.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column),
                                                        std::make_shared<DataTypeString>(),
                                                        "statement"}});
}

} // namespace DB

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

#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Storages/IStorage.h>


namespace DB
{
BlockIO InterpreterExistsQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterExistsQuery::getSampleBlock()
{
    return Block{{ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "result"}};
}


BlockInputStreamPtr InterpreterExistsQuery::executeImpl()
{
    const ASTExistsQuery & ast = typeid_cast<const ASTExistsQuery &>(*query_ptr);
    bool res = ast.temporary ? context.isExternalTableExist(ast.table) : context.isTableExist(ast.database, ast.table);

    return std::make_shared<OneBlockInputStream>(
        Block{{ColumnUInt8::create(1, res), std::make_shared<DataTypeUInt8>(), "result"}});
}

} // namespace DB

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

#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterTruncateQuery.h>
#include <Parsers/ASTTruncateQuery.h>
#include <Storages/IStorage.h>


namespace DB
{
BlockIO InterpreterTruncateQuery::execute()
{
    auto & truncate = typeid_cast<ASTTruncateQuery &>(*query_ptr);

    const String & table_name = truncate.table;
    String database_name = truncate.database.empty() ? context.getCurrentDatabase() : truncate.database;
    StoragePtr table = context.getTable(database_name, table_name);
    table->truncate(query_ptr, context);
    return {};
}

} // namespace DB

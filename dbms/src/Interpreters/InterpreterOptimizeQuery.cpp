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
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Storages/IStorage.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const ASTOptimizeQuery & ast = typeid_cast<const ASTOptimizeQuery &>(*query_ptr);

    if (ast.final && !ast.partition)
        throw Exception("FINAL flag for OPTIMIZE query is meaningful only with specified PARTITION", ErrorCodes::BAD_ARGUMENTS);

    StoragePtr table = context.getTable(ast.database, ast.table);
    auto table_lock = table->lockStructureForShare(RWLock::NO_QUERY);
    table->optimize(query_ptr, ast.partition, ast.final, ast.deduplicate, context);
    return {};
}

} // namespace DB

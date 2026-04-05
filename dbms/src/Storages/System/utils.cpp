// Copyright 2025 PingCAP, Inc.
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
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/System/utils.h>

namespace DB
{

KeyspaceID parseKeyspaceIDFromSelectQueryInfo(const SelectQueryInfo & query_info)
{
    const auto & select = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    const auto & where_expression = select.where_expression;
    if (!where_expression)
        return NullspaceID;
    const auto * func = typeid_cast<const ASTFunction *>(where_expression.get());
    // TODO: support other functions.
    if (!func || func->name != "equals" || func->arguments->children.size() != 2)
        return NullspaceID;
    const auto * identifier = typeid_cast<const ASTIdentifier *>(func->arguments->children[0].get());
    if (!identifier || identifier->name != "keyspace_id")
        return NullspaceID;
    const auto * literal = typeid_cast<const ASTLiteral *>(func->arguments->children[1].get());
    if (!literal)
        return NullspaceID;
    return literal->value.get<KeyspaceID>();
}

} // namespace DB

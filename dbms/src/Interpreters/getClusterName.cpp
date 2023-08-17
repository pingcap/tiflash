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
#include <Interpreters/getClusterName.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}


std::string getClusterName(const IAST & node)
{
    if (const ASTIdentifier * ast_id = typeid_cast<const ASTIdentifier *>(&node))
        return ast_id->name;

    if (const ASTLiteral * ast_lit = typeid_cast<const ASTLiteral *>(&node))
        return ast_lit->value.safeGet<String>();

    if (const ASTFunction * ast_func = typeid_cast<const ASTFunction *>(&node))
    {
        if (!ast_func->range.first || !ast_func->range.second)
            throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);

        return String(ast_func->range.first, ast_func->range.second);
    }

    throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);
}

} // namespace DB

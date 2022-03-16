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

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTDBGInvokeQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDBGInvokeQuery.h>

#include <Common/typeid_cast.h>
#include "ASTFunction.h"

namespace DB
{


bool ParserDBGInvokeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_set("DBGInvoke");

    ASTDBGInvokeQuery::DBGFunc func;

    if (!s_set.ignore(pos, expected))
        return false;

    ParserFunction parser_function;
    ASTPtr function;
    if (!parser_function.parse(pos, function, expected))
        return false;

    func.name = typeid_cast<const ASTFunction &>(*function).name;
    func.args = typeid_cast<const ASTFunction &>(*function).arguments->children;

    auto query = std::make_shared<ASTDBGInvokeQuery>();
    node = query;

    query->func = func;

    return true;
}


}

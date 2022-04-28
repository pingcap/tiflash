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

#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTKillQueryQuery>();

    if (!ParserKeyword{"KILL QUERY"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"WHERE"}.ignore(pos, expected))
        return false;

    ParserExpression p_where_expression;
    if (!p_where_expression.parse(pos, query->where_expression, expected))
        return false;

    if (ParserKeyword{"SYNC"}.ignore(pos))
        query->sync = true;
    else if (ParserKeyword{"ASYNC"}.ignore(pos))
        query->sync = false;
    else if (ParserKeyword{"TEST"}.ignore(pos))
        query->test = true;

    node = std::move(query);

    return true;
}

}

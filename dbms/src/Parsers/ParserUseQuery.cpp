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
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserUseQuery.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use("USE");
    ParserIdentifier name_p;

    ASTPtr database;

    if (!s_use.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, database, expected))
        return false;

    auto query = std::make_shared<ASTUseQuery>();
    query->database = typeid_cast<ASTIdentifier &>(*database).name;
    node = query;

    return true;
}

} // namespace DB

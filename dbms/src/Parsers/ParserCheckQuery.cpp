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

#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table("CHECK TABLE");
    ParserToken s_dot(TokenType::Dot);

    ParserIdentifier table_parser;

    ASTPtr table;
    ASTPtr database;

    if (!s_check_table.ignore(pos, expected))
        return false;
    if (!table_parser.parse(pos, database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
            return false;

        auto query = std::make_shared<ASTCheckQuery>();
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
        node = query;
    }
    else
    {
        table = database;
        auto query = std::make_shared<ASTCheckQuery>();
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
        node = query;
    }

    return true;
}

}

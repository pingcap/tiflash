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

#include <Parsers/ParserTruncateQuery.h>

#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTTruncateQuery.h>


namespace DB
{

bool ParserTruncateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_truncate("TRUNCATE");
    ParserKeyword s_table("TABLE");

    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier table_parser;

    ASTPtr table;
    ASTPtr database;

    auto query = std::make_shared<ASTTruncateQuery>();

    if (!s_truncate.ignore(pos, expected))
        return false;

    if (!s_table.ignore(pos, expected))
        return false;

    if (!table_parser.parse(pos, database, expected))
        return false;

    /// Parse [db].name
    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
            return false;

        query->table = typeid_cast<ASTIdentifier &>(*table).name;
        query->database = typeid_cast<ASTIdentifier &>(*database).name;
    }
    else
    {
        table = database;
        query->table = typeid_cast<ASTIdentifier &>(*table).name;
    }

    node = query;

    return true;
}

}

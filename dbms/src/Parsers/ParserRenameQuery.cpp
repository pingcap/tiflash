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
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>


namespace DB
{
/// Parse database.table or table.
static bool parseDatabaseAndTable(ASTRenameQuery::Table & db_and_table, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);

    ASTPtr database;
    ASTPtr table;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    db_and_table.database = database ? typeid_cast<const ASTIdentifier &>(*database).name : "";
    db_and_table.table = typeid_cast<const ASTIdentifier &>(*table).name;

    return true;
}


bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename_table("RENAME TABLE");
    ParserKeyword s_to("TO");
    ParserToken s_comma(TokenType::Comma);

    if (!s_rename_table.ignore(pos, expected))
        return false;

    ASTRenameQuery::Elements elements;

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos))
            break;

        elements.push_back(ASTRenameQuery::Element());

        if (!parseDatabaseAndTable(elements.back().from, pos, expected) || !s_to.ignore(pos)
            || !parseDatabaseAndTable(elements.back().to, pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTRenameQuery>();
    node = query;

    query->elements = elements;
    return true;
}


} // namespace DB

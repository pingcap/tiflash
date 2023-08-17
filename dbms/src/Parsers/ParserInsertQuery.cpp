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
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}


bool ParserInsertQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // TODO: support partition in sub query

    ParserKeyword s_insert_into("INSERT");
    ParserKeyword s_upsert_into("UPSERT");
    ParserKeyword s_import_into("IMPORT");
    ParserKeyword s_delete("DELETE");
    ParserKeyword s_into("INTO");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_function("FUNCTION");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_values("VALUES");
    ParserKeyword s_format("FORMAT");
    ParserKeyword s_select("SELECT");
    ParserKeyword s_selraw("SELRAW");
    ParserKeyword s_with("WITH");
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserList columns_p(
        std::make_unique<ParserCompoundIdentifier>(),
        std::make_unique<ParserToken>(TokenType::Comma),
        false);
    ParserFunction table_function_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;
    ASTPtr format;
    ASTPtr select;
    ASTPtr table_function;
    /// Insertion data
    const char * data = nullptr;

    bool is_insert = s_insert_into.ignore(pos, expected);
    bool is_upsert = s_upsert_into.ignore(pos, expected);
    bool is_import = s_import_into.ignore(pos, expected);
    if (!is_insert && !is_upsert && !is_import)
        return false;

    bool is_delete = s_delete.ignore(pos, expected);

    if (!s_into.ignore(pos, expected))
        return false;

    s_table.ignore(pos, expected);

    if (s_function.ignore(pos, expected))
    {
        if (!table_function_p.parse(pos, table_function, expected))
            return false;
    }
    else
    {
        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    auto query = std::make_shared<ASTInsertQuery>();
    node = query;

    /// PARTITION p or PARTITION (p1, p2, ...)
    if (s_partition.ignore(pos, expected))
    {
        if (!ParserPartition().parse(pos, query->partition_expression_list, expected))
            return false;
    }

    /// Is there a list of columns
    if (s_lparen.ignore(pos, expected))
    {
        if (!columns_p.parse(pos, columns, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    Pos before_select = pos;

    /// VALUES or FORMAT or SELECT
    if (s_values.ignore(pos, expected))
    {
        data = pos->begin;
    }
    else if (s_format.ignore(pos, expected))
    {
        auto name_pos = pos;

        if (!name_p.parse(pos, format, expected))
            return false;

        data = name_pos->end;

        if (data < end && *data == ';')
            throw Exception(
                "You have excessive ';' symbol before data for INSERT.\n"
                "Example:\n\n"
                "INSERT INTO t (x, y) FORMAT TabSeparated\n"
                ";\tHello\n"
                "2\tWorld\n"
                "\n"
                "Note that there is no ';' just after format name, "
                "you need to put at least one whitespace symbol before the data.",
                ErrorCodes::SYNTAX_ERROR);

        while (data < end && (*data == ' ' || *data == '\t' || *data == '\f'))
            ++data;

        /// Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.

        if (data < end && *data == '\r')
            ++data;

        if (data < end && *data == '\n')
            ++data;
    }
    else if (s_select.ignore(pos, expected) || s_selraw.ignore(pos, expected) || s_with.ignore(pos, expected))
    {
        pos = before_select;
        ParserSelectWithUnionQuery select_p;
        select_p.parse(pos, select, expected);
    }
    else
    {
        return false;
    }

    if (table_function)
    {
        query->table_function = table_function;
    }
    else
    {
        if (database)
            query->database = typeid_cast<ASTIdentifier &>(*database).name;

        query->table = typeid_cast<ASTIdentifier &>(*table).name;
    }

    if (format)
        query->format = typeid_cast<ASTIdentifier &>(*format).name;

    query->columns = columns;
    query->select = select;
    query->data = data != end ? data : nullptr;
    query->end = end;
    query->is_import = is_import;
    query->is_upsert = is_upsert;
    query->is_delete = is_delete;

    if (columns)
        query->children.push_back(columns);
    if (select)
        query->children.push_back(select);

    return true;
}


} // namespace DB

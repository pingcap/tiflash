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

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}


bool ParserSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    ParserKeyword s_select("SELECT");
    ParserKeyword s_selraw("SELRAW");
    ParserKeyword s_no_kvstore("NOKVSTORE");
    ParserKeyword s_distinct("DISTINCT");
    ParserKeyword s_from("FROM");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_segment("SEGMENT");
    ParserKeyword s_prewhere("PREWHERE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_group_by("GROUP BY");
    ParserKeyword s_with("WITH");
    ParserKeyword s_totals("TOTALS");
    ParserKeyword s_having("HAVING");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_limit("LIMIT");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_by("BY");

    ParserNotEmptyExpressionList exp_list(false);
    ParserNotEmptyExpressionList exp_list_for_with_clause(
        false,
        true); /// Set prefer_alias_to_column_name for each alias.
    ParserNotEmptyExpressionList exp_list_for_select_clause(true); /// Allows aliases without AS keyword.
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserOrderByExpressionList order_list;

    /// WITH expr list
    {
        if (s_with.ignore(pos, expected))
        {
            if (!exp_list_for_with_clause.parse(pos, select_query->with_expression_list, expected))
                return false;
        }
    }

    /// SELECT [DISTINCT] expr list
    {
        bool is_select = s_select.ignore(pos, expected);
        bool is_selraw = s_selraw.ignore(pos, expected);
        if (!is_select && !is_selraw)
            return false;
        select_query->raw_for_mutable = is_selraw;

        if (s_no_kvstore.ignore(pos, expected))
            select_query->no_kvstore = true;

        if (s_distinct.ignore(pos, expected))
            select_query->distinct = true;

        if (!exp_list_for_select_clause.parse(pos, select_query->select_expression_list, expected))
            return false;
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction(...)
    if (s_from.ignore(pos, expected))
    {
        if (!ParserTablesInSelectQuery().parse(pos, select_query->tables, expected))
            return false;
    }

    /// PARTITION p or PARTITION (p1, p2, ...)
    if (s_partition.ignore(pos, expected))
    {
        if (!ParserPartition().parse(pos, select_query->partition_expression_list, expected))
            return false;
    }

    /// SEGMENT p or SEGMENT (p1, p2, ...)
    if (s_segment.ignore(pos, expected))
    {
        if (!ParserPartition().parse(pos, select_query->segment_expression_list, expected))
            return false;
    }

    /// PREWHERE expr
    if (s_prewhere.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, select_query->prewhere_expression, expected))
            return false;
    }

    /// WHERE expr
    if (s_where.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, select_query->where_expression, expected))
            return false;
    }

    /// GROUP BY expr list
    if (s_group_by.ignore(pos, expected))
    {
        if (!exp_list.parse(pos, select_query->group_expression_list, expected))
            return false;
    }

    /// WITH TOTALS
    if (s_with.ignore(pos, expected))
    {
        if (!s_totals.ignore(pos, expected))
            return false;

        throw Exception("WITH TOTALS is no longer supported");
    }

    /// HAVING expr
    if (s_having.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, select_query->having_expression, expected))
            return false;
    }

    /// ORDER BY expr ASC|DESC COLLATE 'locale' list
    if (s_order_by.ignore(pos, expected))
    {
        if (!order_list.parse(pos, select_query->order_expression_list, expected))
            return false;
    }

    /// LIMIT length | LIMIT offset, length | LIMIT count BY expr-list
    if (s_limit.ignore(pos, expected))
    {
        ParserToken s_comma(TokenType::Comma);
        ParserNumber num;

        if (!num.parse(pos, select_query->limit_length, expected))
            return false;

        if (s_comma.ignore(pos, expected))
        {
            select_query->limit_offset = select_query->limit_length;
            if (!num.parse(pos, select_query->limit_length, expected))
                return false;
        }
        else if (s_by.ignore(pos, expected))
        {
            select_query->limit_by_value = select_query->limit_length;
            select_query->limit_length = nullptr;

            if (!exp_list.parse(pos, select_query->limit_by_expression_list, expected))
                return false;
        }
    }

    /// LIMIT length | LIMIT offset, length
    if (s_limit.ignore(pos, expected))
    {
        if (!select_query->limit_by_value || select_query->limit_length)
            return false;

        ParserToken s_comma(TokenType::Comma);
        ParserNumber num;

        if (!num.parse(pos, select_query->limit_length, expected))
            return false;

        if (s_comma.ignore(pos, expected))
        {
            select_query->limit_offset = select_query->limit_length;
            if (!num.parse(pos, select_query->limit_length, expected))
                return false;
        }
    }

    /// SETTINGS key1 = value1, key2 = value2, ...
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, select_query->settings, expected))
            return false;
    }

    if (select_query->with_expression_list)
        select_query->children.push_back(select_query->with_expression_list);
    select_query->children.push_back(select_query->select_expression_list);
    if (select_query->tables)
        select_query->children.push_back(select_query->tables);
    if (select_query->prewhere_expression)
        select_query->children.push_back(select_query->prewhere_expression);
    if (select_query->where_expression)
        select_query->children.push_back(select_query->where_expression);
    if (select_query->group_expression_list)
        select_query->children.push_back(select_query->group_expression_list);
    if (select_query->having_expression)
        select_query->children.push_back(select_query->having_expression);
    if (select_query->order_expression_list)
        select_query->children.push_back(select_query->order_expression_list);
    if (select_query->limit_by_value)
        select_query->children.push_back(select_query->limit_by_value);
    if (select_query->limit_by_expression_list)
        select_query->children.push_back(select_query->limit_by_expression_list);
    if (select_query->limit_offset)
        select_query->children.push_back(select_query->limit_offset);
    if (select_query->limit_length)
        select_query->children.push_back(select_query->limit_length);
    if (select_query->settings)
        select_query->children.push_back(select_query->settings);

    return true;
}

} // namespace DB

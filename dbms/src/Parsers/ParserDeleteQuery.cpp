#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ParserPartition.h>

#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_delete_from("DELETE FROM");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_where("WHERE");
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr where;

    ParserExpressionWithOptionalAlias exp_elem(false);

    if (!s_delete_from.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    std::shared_ptr<ASTDeleteQuery> query = std::make_shared<ASTDeleteQuery>();
    node = query;

    /// PARTITION p or PARTITION (p1, p2, ...)
    if (s_partition.ignore(pos, expected))
    {
        if (!ParserPartition().parse(pos, query->partition_expression_list, expected))
            return false;
    }

    if (!s_where.ignore(pos, expected))
        return false;

    if (!exp_elem.parse(pos, where, expected))
        return false;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;

    query->table = typeid_cast<ASTIdentifier &>(*table).name;

    // TODO: Support syntax without 'where'

    if (where)
    {
        query->where = where;
        query->children.push_back(where);

        std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();

        std::shared_ptr<ASTAsterisk> asterisk = std::make_shared<ASTAsterisk>();
        std::shared_ptr<ASTExpressionList> table_columns = std::make_shared<ASTExpressionList>();
        table_columns->children.push_back(asterisk);

        select_query->select_expression_list = table_columns;
        select_query->children.push_back(table_columns);

        auto table_expr = std::make_shared<ASTTableExpression>();
        table_expr->database_and_table_name =
            std::make_shared<ASTIdentifier>(
                query->database.size() ? query->database + '.' + query->table : query->table,
                ASTIdentifier::Table);
        if(!query->database.empty())
        {
            table_expr->database_and_table_name->children.emplace_back(std::make_shared<ASTIdentifier>(query->database, ASTIdentifier::Database));
            table_expr->database_and_table_name->children.emplace_back(std::make_shared<ASTIdentifier>(query->table, ASTIdentifier::Table));
        }

        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->table_expression = table_expr;
        table_element->children.emplace_back(table_expr);

        auto table_list = std::make_shared<ASTTablesInSelectQuery>();
        table_list->children.emplace_back(table_element);

        select_query->tables = table_list;
        select_query->children.push_back(table_list);

        select_query->where_expression = query->where;
        select_query->children.push_back(query->where);
        select_query->partition_expression_list = query->partition_expression_list;

        query->select = select_query;
        query->children.push_back(select_query);
    }

    return true;
}


}

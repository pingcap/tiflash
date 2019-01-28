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

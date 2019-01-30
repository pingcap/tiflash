#include <Parsers/CommonParsers.h>
#include <Parsers/ParserManageQuery.h>
#include <Parsers/ParserPartition.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTManageQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{
bool ParserManageQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_manage_table("MANAGE TABLE");
    ParserKeyword s_flush("FLUSH");
    ParserKeyword s_status("STATUS");
    ParserKeyword s_check("CHECK");

    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ASTPtr database;
    ASTPtr table;
    ManageOperation::Enum operation;

    if (!s_manage_table.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    if (s_flush.ignore(pos, expected))
        operation = ManageOperation::Enum::Flush;
    else if (s_status.ignore(pos, expected))
        operation = ManageOperation::Enum::Status;
    else if (s_check.ignore(pos, expected))
        operation = ManageOperation::Enum::Check;
    else
        return false;

    auto query = std::make_shared<ASTManageQuery>();
    node = query;

    if (database)
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;

    query->operation = operation;

    return true;
}
}

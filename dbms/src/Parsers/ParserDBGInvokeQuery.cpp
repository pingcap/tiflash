#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTDBGInvokeQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDBGInvokeQuery.h>

#include <Common/typeid_cast.h>
#include "ASTFunction.h"

namespace DB
{


bool ParserDBGInvokeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_set("DBGInvoke");

    ASTDBGInvokeQuery::DBGFunc func;

    if (!s_set.ignore(pos, expected))
        return false;

    ParserFunction parser_function;
    ASTPtr function;
    if (!parser_function.parse(pos, function, expected))
        return false;

    func.name = typeid_cast<const ASTFunction &>(*function).name;
    func.args = typeid_cast<const ASTFunction &>(*function).arguments->children;

    auto query = std::make_shared<ASTDBGInvokeQuery>();
    node = query;

    query->func = func;

    return true;
}


}

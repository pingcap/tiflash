#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

class ParserManageQuery : public IParserBase
{
protected:
    const char * getName() const { return "MANAGE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}

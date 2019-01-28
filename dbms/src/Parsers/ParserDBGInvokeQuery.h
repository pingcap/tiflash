#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

// Invoke inner functions, for debug only.
class ParserDBGInvokeQuery : public IParserBase
{
public:
    explicit ParserDBGInvokeQuery() {}

protected:
    const char * getName() const override { return "DBGInvoke query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

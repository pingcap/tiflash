#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query USE db
  */
class ParserTruncateQuery : public IParserBase
{
protected:
  const char * getName() const { return "TRUNCATE query"; }
  bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}

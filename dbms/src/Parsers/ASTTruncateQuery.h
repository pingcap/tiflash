#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTTruncateQuery : public IAST
{
public:

  String database;
  String table;

  /** Get the text that identifies this element. */
  String getID() const override { return "Truncate_" + database + "_" + table; };

  ASTPtr clone() const override { return std::make_shared<ASTTruncateQuery>(*this); }

protected:
  void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
  {
      settings.ostr << (settings.hilite ? hilite_keyword : "") << "TRUNCATE TABLE " << (settings.hilite ? hilite_none : "");
      if (!database.empty())
      {
          settings.ostr << backQuoteIfNeed(database);
          settings.ostr << ".";
      }
      settings.ostr << backQuoteIfNeed(table);
  }
};

}

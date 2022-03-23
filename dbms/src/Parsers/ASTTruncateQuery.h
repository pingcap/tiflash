// Copyright 2022 PingCAP, Ltd.
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

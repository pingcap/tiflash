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

#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

#include <iomanip>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases{false};
    bool temporary{false};
    String from;
    String like;
    bool not_like{false};

    /** Get the text that identifies this element. */
    String getID() const override { return "ShowTables"; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTShowTablesQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (databases)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES"
                          << (settings.hilite ? hilite_none : "");
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW TABLES"
                          << (settings.hilite ? hilite_none : "");

            if (!from.empty())
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM "
                              << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(from);

            if (!like.empty())
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIKE "
                              << (settings.hilite ? hilite_none : "") << std::quoted(like, '\'');
        }
    }
};

} // namespace DB

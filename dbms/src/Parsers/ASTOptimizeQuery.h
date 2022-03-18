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


/** OPTIMIZE query
  */
class ASTOptimizeQuery : public IAST
{
public:
    String database;
    String table;

    /// The partition to optimize can be specified.
    ASTPtr partition;
    /// A flag can be specified - perform optimization "to the end" instead of one step.
    bool final;
    /// Do deduplicate (default: false)
    bool deduplicate;

    /** Get the text that identifies this element. */
    String getID() const override { return "OptimizeQuery_" + database + "_" + table + (final ? "_final" : "") + (deduplicate ? "_deduplicate" : ""); };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTOptimizeQuery>(*this);
        res->children.clear();

        if (partition)
        {
            res->partition = partition->clone();
            res->children.push_back(res->partition);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }

        if (final)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FINAL" << (settings.hilite ? hilite_none : "");

        if (deduplicate)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " DEDUPLICATE" << (settings.hilite ? hilite_none : "");
    }
};

}

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

#include <chrono>

namespace DB
{
/** DROP query
  */
class ASTDropQuery : public ASTQueryWithOutput
{
public:
    bool detach{false}; /// DETACH query, not DROP.
    bool if_exists{false};
    bool temporary{false};
    String database;
    String table;
    // Timeout for acquring drop lock on storage
    std::chrono::milliseconds lock_timeout{std::chrono::milliseconds(0)};

    /** Get the text that identifies this element. */
    String getID() const override { return (detach ? "DetachQuery_" : "DropQuery_") + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDropQuery>(*this);
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (table.empty() && !database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << (detach ? "DETACH DATABASE " : "DROP DATABASE ")
                          << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "")
                          << backQuoteIfNeed(database);
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << (detach ? "DETACH TABLE " : "DROP TABLE ")
                          << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "")
                          << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
        }
    }
};

} // namespace DB

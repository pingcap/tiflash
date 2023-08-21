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

#include <Parsers/IAST.h>


namespace DB
{
namespace ManageOperation
{
enum Enum
{
    Flush,
    Status,
    Check,
    DeleteRows,
    MergeDelta,
};

inline const char * toString(UInt64 op)
{
    static const char * data[] = {"Flush", "Status", "Check", "Delete Rows", "Merge Delta"};
    return op < 5 ? data[op] : "Unknown operation";
}
} // namespace ManageOperation

/** Manage query
  */
class ASTManageQuery : public IAST
{
public:
    String database;
    String table;

    ManageOperation::Enum operation;

    size_t rows = 0;

    /** Get the text that identifies this element. */
    String getID() const override
    {
        return "ManageQuery_" + database + "_" + table + "_" + ManageOperation::toString(operation);
    };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTManageQuery>(*this);
        res->children.clear();
        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/)
        const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MANAGE TABLE "
                      << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table) << " "
                      << (settings.hilite ? hilite_keyword : "") << ManageOperation::toString(operation)
                      << (settings.hilite ? hilite_none : "");
        if (operation == ManageOperation::Enum::DeleteRows)
            settings.ostr << " " << rows;
    }
};
} // namespace DB

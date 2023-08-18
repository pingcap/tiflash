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
#include <Common/FieldVisitors.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>


namespace DB
{
/** SET query
  */
class ASTSetQuery : public IAST
{
public:
    bool is_standalone = true; /// If false, this AST is a part of another query, such as SELECT.

    struct Change
    {
        String name;
        Field value;
    };

    using Changes = std::vector<Change>;
    Changes changes;

    /** Get the text that identifies this element. */
    String getID() const override { return "Set"; };

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (is_standalone)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "SET " << (settings.hilite ? hilite_none : "");

        for (ASTSetQuery::Changes::const_iterator it = changes.begin(); it != changes.end(); ++it)
        {
            if (it != changes.begin())
                settings.ostr << ", ";

            settings.ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
        }
    }
};

} // namespace DB

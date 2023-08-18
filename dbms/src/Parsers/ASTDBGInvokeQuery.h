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


/** DBGInvoke query
  */
class ASTDBGInvokeQuery : public IAST
{
public:
    struct DBGFunc
    {
        String name;
        ASTs args;
    };

    DBGFunc func;

    /** Get the text that identifies this element. */
    String getID() const override { return "DBGInvoke"; };

    ASTPtr clone() const override { return std::make_shared<ASTDBGInvokeQuery>(*this); }

    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DBGInvoke " << (settings.hilite ? hilite_none : "")
                      << func.name << "(";

        for (auto it = func.args.begin(); it != func.args.end(); ++it)
        {
            if (it != func.args.begin())
                settings.ostr << ", ";
            settings.ostr << (*it)->getColumnName();
        }

        settings.ostr << ")";
    }
};

} // namespace DB

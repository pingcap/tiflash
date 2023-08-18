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

#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>


namespace DB
{


class ASTEnumElement : public IAST
{
public:
    String name;
    Field value;

    ASTEnumElement(const String & name, const Field & value)
        : name{name}
        , value{value}
    {}

    String getID() const override { return "EnumElement"; }

    ASTPtr clone() const override { return std::make_shared<ASTEnumElement>(name, value); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;

        const std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        settings.ostr << settings.nl_or_ws << indent_str << '\'' << name
                      << "' = " << applyVisitor(FieldVisitorToString{}, value);
    }
};


} // namespace DB

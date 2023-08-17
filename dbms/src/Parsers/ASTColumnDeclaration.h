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

/** Name, type, default-specifier, default-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    ASTPtr type;
    String default_specifier;
    ASTPtr default_expression;

    String getID() const override { return "ColumnDeclaration_" + name; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTColumnDeclaration>(*this);
        ASTPtr ptr{res};

        res->children.clear();

        if (type)
        {
            res->type = type;
            res->children.push_back(res->type);
        }

        if (default_expression)
        {
            res->default_expression = default_expression->clone();
            res->children.push_back(res->default_expression);
        }

        return ptr;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

        settings.ostr << settings.nl_or_ws << indent_str << backQuoteIfNeed(name);
        if (type)
        {
            settings.ostr << ' ';
            type->formatImpl(settings, state, frame);
        }

        if (default_expression)
        {
            settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << default_specifier
                          << (settings.hilite ? hilite_none : "") << ' ';
            default_expression->formatImpl(settings, state, frame);
        }
    }
};

} // namespace DB

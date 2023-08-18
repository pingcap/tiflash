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

#include <Parsers/ASTWithAlias.h>


namespace DB
{


/** SELECT subquery
  */
class ASTSubquery : public ASTWithAlias
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "Subquery"; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTSubquery>(*this);
        ASTPtr ptr{res};

        res->children.clear();

        for (const auto & child : children)
            res->children.emplace_back(child->clone());

        return ptr;
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame)
        const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        settings.ostr << nl_or_nothing << indent_str << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        children[0]->formatImpl(settings, state, frame_nested);
        settings.ostr << nl_or_nothing << indent_str << ")";
    }

    String getColumnNameImpl() const override;
};

} // namespace DB

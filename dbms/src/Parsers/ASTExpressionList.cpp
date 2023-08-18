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

#include <Parsers/ASTExpressionList.h>


namespace DB
{

ASTPtr ASTExpressionList::clone() const
{
    const auto res = std::make_shared<ASTExpressionList>(*this);
    res->children.clear();

    for (const auto & child : children)
        res->children.emplace_back(child->clone());

    return res;
}

void ASTExpressionList::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            settings.ostr << ", ";

        (*it)->formatImpl(settings, state, frame);
    }
}

void ASTExpressionList::formatImplMultiline(
    const FormatSettings & settings,
    FormatState & state,
    FormatStateStacked frame) const
{
    std::string indent_str = "\n" + std::string(4 * (frame.indent + 1), ' ');

    ++frame.indent;
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            settings.ostr << ", ";

        if (children.size() > 1)
            settings.ostr << indent_str;

        (*it)->formatImpl(settings, state, frame);
    }
}

} // namespace DB

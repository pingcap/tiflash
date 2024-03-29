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

#include <IO/Buffer/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

void ASTWithAlias::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!alias.empty())
    {
        /// If we have previously output this node elsewhere in the query, now it is enough to output only the alias.
        if (!state.printed_asts_with_alias.emplace(frame.current_select, alias).second)
        {
            WriteBufferFromOStream wb(settings.ostr, 32);
            writeProbablyBackQuotedString(alias, wb);
            return;
        }
    }

    /// If there is an alias, then parentheses are required around the entire expression, including the alias. Because a record of the form `0 AS x + 0` is syntactically invalid.
    if (frame.need_parens && !alias.empty())
        settings.ostr << '(';

    formatImplWithoutAlias(settings, state, frame);

    if (!alias.empty())
    {
        writeAlias(alias, settings.ostr, settings.hilite);
        if (frame.need_parens)
            settings.ostr << ')';
    }
}

} // namespace DB

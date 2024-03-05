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
#include <Parsers/ASTIdentifier.h>


namespace DB
{

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    auto format_element = [&](const String & name) {
        settings.ostr << (settings.hilite ? hilite_identifier : "");

        WriteBufferFromOStream wb(settings.ostr, 32);
        writeProbablyBackQuotedString(name, wb);
        wb.next();

        settings.ostr << (settings.hilite ? hilite_none : "");
    };

    /// A simple or compound identifier?

    if (children.size() > 1)
    {
        for (size_t i = 0, size = children.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << '.';

            format_element(static_cast<const ASTIdentifier &>(*children[i].get()).name);
        }
    }
    else
    {
        format_element(name);
    }
}

} // namespace DB

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

#include <Parsers/ASTOrderByElement.h>


namespace DB
{
void ASTOrderByElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << (direction == -1 ? " DESC" : " ASC")
                  << (settings.hilite ? hilite_none : "");

    if (nulls_direction_was_explicitly_specified)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " NULLS "
                      << (nulls_direction == direction ? "LAST" : "FIRST") << (settings.hilite ? hilite_none : "");
    }

    if (collation)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " COLLATE " << (settings.hilite ? hilite_none : "");
        collation->formatImpl(settings, state, frame);
    }
}

} // namespace DB

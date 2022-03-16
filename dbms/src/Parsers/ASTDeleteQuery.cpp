// Copyright 2022 PingCAP, Ltd.
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

#include <iomanip>
#include <Parsers/ASTDeleteQuery.h>


namespace DB
{

void ASTDeleteQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.ostr
        << (settings.hilite ? hilite_keyword : "")
        << "DELETE FROM "
        << (settings.hilite ? hilite_none : "")
        << (!database.empty() ? backQuoteIfNeed(database) + "." : "")
        << backQuoteIfNeed(table);

    if (partition_expression_list)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws <<
            indent_str << "PARTITION " << (settings.hilite ? hilite_none : "");
        partition_expression_list->formatImpl(settings, state, frame);
    }

    if (where)
    {
        settings.ostr << " WHERE ";
        where->formatImpl(settings, state, frame);
    }
}

}

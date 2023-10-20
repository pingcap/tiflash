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

#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{
/// Parse `name = value`.
static bool parseNameValuePair(ASTSetQuery::Change & change, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (!value_p.parse(pos, value, expected))
        return false;

    change.name = typeid_cast<const ASTIdentifier &>(*name).name;
    change.value = typeid_cast<const ASTLiteral &>(*value).value;

    return true;
}

bool ParserSetQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    if (!parse_only_internals)
    {
        ParserKeyword s_set("SET");

        if (!s_set.ignore(pos, expected))
            return false;
    }

    ASTSetQuery::Changes changes;

    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(ASTSetQuery::Change());

        if (!parseNameValuePair(changes.back(), pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTSetQuery>();
    node = query;

    query->is_standalone = !parse_only_internals;
    query->changes = changes;

    return true;
}


} // namespace DB

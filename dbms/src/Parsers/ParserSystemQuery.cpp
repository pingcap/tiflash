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

#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB
{


bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;
    for (int i = static_cast<int>(Type::UNKNOWN) + 1; i < static_cast<int>(Type::END); ++i)
    {
        Type t = static_cast<Type>(i);
        if (ParserKeyword{ASTSystemQuery::typeToString(t)}.ignore(pos))
        {
            res->type = t;
            found = true;
        }
    }

    if (!found)
        return false;

    if (res->type == Type::RELOAD_DICTIONARY)
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, res->target_dictionary))
            return false;
    }
    else if (res->type == Type::SYNC_REPLICA)
    {
        throw Exception("SYNC REPLICA is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
    }

    node = std::move(res);
    return true;
}

}

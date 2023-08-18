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

#include "parseIdentifierOrStringLiteral.h"

#include <Common/typeid_cast.h>

#include "ASTIdentifier.h"
#include "ASTLiteral.h"
#include "ExpressionElementParsers.h"

namespace DB
{

bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    ASTPtr res;

    if (!ParserIdentifier().parse(pos, res, expected))
    {
        if (!ParserStringLiteral().parse(pos, res, expected))
            return false;

        result = typeid_cast<const ASTLiteral &>(*res).value.safeGet<String>();
    }
    else
        result = typeid_cast<const ASTIdentifier &>(*res).name;

    return true;
}

} // namespace DB

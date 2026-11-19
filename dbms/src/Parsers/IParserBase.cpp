// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Parsers/IParserBase.cpp
//
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

#include <Common/Exception.h>
#include <Parsers/IParserBase.h>

#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
extern const int TOO_DEEP_RECURSION;
}


bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto & depth = pos.getParserDepth();
    if (depth >= maxParserDepth)
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Maximum parser depth exceeded ({} calls)", maxParserDepth);
    ++depth;
    SCOPE_EXIT({ --depth; });

    Pos begin = pos;
    expected.add(pos, getName());

    bool res = parseImpl(pos, node, expected);

    if (!res)
    {
        node = nullptr;
        pos = begin;
    }
    else if (node)
        node->range = StringRange(begin, pos);

    return res;
}

} // namespace DB

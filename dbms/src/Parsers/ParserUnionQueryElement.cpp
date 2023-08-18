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
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserSubquery().parse(pos, node, expected) && !ParserSelectQuery().parse(pos, node, expected))
        return false;

    if (auto * ast_sub_query = typeid_cast<ASTSubquery *>(node.get()))
        node = ast_sub_query->children.at(0);

    return true;
}

} // namespace DB

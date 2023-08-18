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
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserUnionQueryElement.h>


namespace DB
{

static void getSelectsFromUnionListNode(ASTPtr & ast_select, ASTs & selects)
{
    if (ASTSelectWithUnionQuery * inner_union = typeid_cast<ASTSelectWithUnionQuery *>(ast_select.get()))
    {
        for (auto & child : inner_union->list_of_selects->children)
            getSelectsFromUnionListNode(child, selects);

        return;
    }

    selects.push_back(std::move(ast_select));
}


bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;

    ParserList parser(std::make_unique<ParserUnionQueryElement>(), std::make_unique<ParserKeyword>("UNION ALL"), false);
    if (!parser.parse(pos, list_node, expected))
        return false;

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    node = select_with_union_query;
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    // flatten inner union query
    for (auto & child : list_node->children)
        getSelectsFromUnionListNode(child, select_with_union_query->list_of_selects->children);

    return true;
}

} // namespace DB

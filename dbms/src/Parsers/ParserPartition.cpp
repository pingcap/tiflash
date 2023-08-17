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
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPartition.h>

namespace DB
{

bool ParserPartition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_id("ID");
    ParserStringLiteral parser_string_literal;
    ParserExpression parser_expr;

    Pos begin = pos;

    auto partition = std::make_shared<ASTPartition>();

    if (s_id.ignore(pos, expected))
    {
        ASTPtr partition_id;
        if (!parser_string_literal.parse(pos, partition_id, expected))
            return false;

        partition->id = dynamic_cast<const ASTLiteral &>(*partition_id).value.get<String>();
    }
    else
    {
        ASTPtr value;
        if (!parser_expr.parse(pos, value, expected))
            return false;

        size_t fields_count;
        StringRef fields_str;

        const auto * tuple_ast = typeid_cast<const ASTFunction *>(value.get());
        if (tuple_ast && tuple_ast->name == "tuple")
        {
            const auto * arguments_ast = dynamic_cast<const ASTExpressionList *>(tuple_ast->arguments.get());
            if (arguments_ast)
                fields_count = arguments_ast->children.size();
            else
                fields_count = 0;

            Pos left_paren = begin;
            Pos right_paren = pos;

            while (left_paren != right_paren && left_paren->type != TokenType::OpeningRoundBracket)
                ++left_paren;
            if (left_paren->type != TokenType::OpeningRoundBracket)
                return false;

            while (right_paren != left_paren && right_paren->type != TokenType::ClosingRoundBracket)
                --right_paren;
            if (right_paren->type != TokenType::ClosingRoundBracket)
                return false;

            fields_str = StringRef(left_paren->end, right_paren->begin - left_paren->end);
        }
        else
        {
            fields_count = 1;
            fields_str = StringRef(begin->begin, pos->begin - begin->begin);
        }

        partition->value = value;
        partition->children.push_back(value);
        partition->fields_str = fields_str;
        partition->fields_count = fields_count;
    }

    node = partition;
    return true;
}

} // namespace DB

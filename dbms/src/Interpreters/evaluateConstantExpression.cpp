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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


std::pair<Field, std::shared_ptr<const IDataType>> evaluateConstantExpression(
    const ASTPtr & node,
    const Context & context)
{
    ExpressionActionsPtr expr_for_constant_folding
        = ExpressionAnalyzer(
              node,
              context,
              nullptr,
              NamesAndTypesList{{"_dummy", std::make_shared<DataTypeUInt8>()}},
              Names())
              .getConstActions();

    /// There must be at least one column in the block so that it knows the number of rows.
    Block block_with_constants{
        {ColumnConst::create(ColumnUInt8::create(1, 0), 1), std::make_shared<DataTypeUInt8>(), "_dummy"}};

    expr_for_constant_folding->execute(block_with_constants);

    if (!block_with_constants || block_with_constants.rows() == 0)
        throw Exception(
            "Logical error: empty block after evaluation of constant expression for IN or VALUES",
            ErrorCodes::LOGICAL_ERROR);

    String name = node->getColumnName();

    if (!block_with_constants.has(name))
        throw Exception(
            "Element of set in IN or VALUES is not a constant expression: " + name,
            ErrorCodes::BAD_ARGUMENTS);

    const ColumnWithTypeAndName & result = block_with_constants.getByName(name);
    const IColumn & result_column = *result.column;

    if (!result_column.isColumnConst())
        throw Exception(
            "Element of set in IN or VALUES is not a constant expression: " + name,
            ErrorCodes::BAD_ARGUMENTS);

    return std::make_pair(result_column[0], result.type);
}


ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, const Context & context)
{
    if (typeid_cast<const ASTLiteral *>(node.get()))
        return node;

    return std::make_shared<ASTLiteral>(evaluateConstantExpression(node, context).first);
}


ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, const Context & context)
{
    if (auto id = typeid_cast<const ASTIdentifier *>(node.get()))
        return std::make_shared<ASTLiteral>(Field(id->name));

    return evaluateConstantExpressionAsLiteral(node, context);
}

} // namespace DB

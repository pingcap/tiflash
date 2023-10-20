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
#include <Common/typeid_cast.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionNumbers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StoragePtr TableFunctionNumbers::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

    if (args_func.size() != 1)
        throw Exception(
            "Table function 'numbers' requires exactly one argument: amount of numbers.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

    if (args.size() != 1)
        throw Exception(
            "Table function 'numbers' requires exactly one argument: amount of numbers.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

    UInt64 limit = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<UInt64>();

    auto res = StorageSystemNumbers::create(getName(), false, limit);
    res->startup();
    return res;
}


void registerTableFunctionNumbers(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNumbers>();
}

} // namespace DB

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

#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageCatBoostPool.h>
#include <TableFunctions/TableFunctionCatBoostPool.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


StoragePtr TableFunctionCatBoostPool::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

    std::string err = "Table function '" + getName() + "' requires 2 parameters: "
        + "column descriptions file, dataset description file";

    if (args_func.size() != 1)
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

    if (args.size() != 2)
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto getStringLiteral = [](const IAST & node, const char * description) {
        auto lit = typeid_cast<const ASTLiteral *>(&node);
        if (!lit)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        if (lit->value.getType() != Field::Types::String)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        return safeGet<const String &>(lit->value);
    };
    String column_descriptions_file = getStringLiteral(*args[0], "Column descriptions file");
    String dataset_description_file = getStringLiteral(*args[1], "Dataset description file");

    return StorageCatBoostPool::create(context, column_descriptions_file, dataset_description_file);
}

void registerTableFunctionCatBoostPool(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCatBoostPool>();
}

} // namespace DB

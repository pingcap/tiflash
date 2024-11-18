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
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_STORAGE;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
extern const int ENGINE_REQUIRED;
extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
extern const int BAD_ARGUMENTS;
extern const int DATA_TYPE_CANNOT_BE_USED_IN_TABLES;
} // namespace ErrorCodes


/// Some types are only for intermediate values of expressions and cannot be used in tables.
static void checkAllTypesAreAllowedInTable(const NamesAndTypesList & names_and_types)
{
    for (const auto & elem : names_and_types)
        if (elem.type->cannotBeStoredInTables())
            throw Exception(
                ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_TABLES,
                "Data type {} cannot be used in tables",
                elem.type->getName());
}


void StorageFactory::registerStorage(const std::string & name, Creator creator)
{
    RUNTIME_CHECK_MSG(
        storages.emplace(name, std::move(creator)).second,
        "TableFunctionFactory: the table function name '{}' is not unique",
        name);
}


StoragePtr StorageFactory::get(
    ASTCreateQuery & query,
    const String & data_path,
    const String & table_name,
    const String & database_name,
    const String & database_engine,
    Context & local_context,
    Context & context,
    const ColumnsDescription & columns,
    bool attach,
    bool has_force_restore_data_flag) const
{
    ASTStorage * storage_def = query.storage;

    /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
    /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
    checkAllTypesAreAllowedInTable(columns.getAll());

    if (!storage_def)
        throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

    const ASTFunction & engine_def = *storage_def->engine;
    if (engine_def.parameters)
        throw Exception(
            "Engine definition cannot take the form of a parametric function",
            ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);
    ASTs args;
    if (engine_def.arguments)
        args = engine_def.arguments->children;

    String name = engine_def.name;

    if ((storage_def->partition_by || storage_def->order_by || storage_def->sample_by || storage_def->settings)
        && !endsWith(name, "MergeTree"))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Engine {} doesn't support PARTITION BY, ORDER BY, SAMPLE BY or SETTINGS clauses. "
            "Currently only the MergeTree family of engines supports them",
            name);
    }

    auto it = storages.find(name);
    if (it == storages.end())
        throw Exception("Unknown table engine " + name, ErrorCodes::UNKNOWN_STORAGE);

    Arguments arguments{
        .engine_name = name,
        .engine_args = args,
        .storage_def = storage_def,
        .query = query,
        .data_path = data_path,
        .table_name = table_name,
        .database_name = database_name,
        .database_engine = database_engine,
        .local_context = local_context,
        .context = context,
        .columns = columns,
        .attach = attach,
        .has_force_restore_data_flag = has_force_restore_data_flag};

    return it->second(arguments);
}

} // namespace DB

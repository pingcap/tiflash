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

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageFactory.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

static ASTPtr extractKeyExpressionList(IAST & node)
{
    // For multiple primary key, this is a ASTFunction with name "tuple".
    // For single primary key, this is a ASTExpressionList.
    const auto * expr_func = typeid_cast<const ASTFunction *>(&node);
    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple.
        return expr_func->children.at(0);
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node.ptr());
        return res;
    }
}

static String getDeltaMergeVerboseHelp()
{
    String help = R"(

DeltaMerge requires:
- primary key
- an extra table info parameter in JSON format
- in most cases, it should be created implicitly through raft rather than explicitly
- tombstone, default to 0

Examples of creating a DeltaMerge table:
- Create Table ... engine = DeltaMerge((CounterID, EventDate)) # JSON format table info is set to empty string and tombstone is 0
- Create Table ... engine = DeltaMerge((CounterID, EventDate), '{JSON format table info}')
- Create Table ... engine = DeltaMerge((CounterID, EventDate), '{JSON format table info}', 1)
)";
    return help;
}

void registerStorageDeltaMerge(StorageFactory & factory)
{
    factory.registerStorage("DeltaMerge", [](const StorageFactory::Arguments & args) {
        if (args.engine_args.size() > 3 || args.engine_args.empty())
            throw Exception(getDeltaMergeVerboseHelp(), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTPtr primary_expr_list = extractKeyExpressionList(*args.engine_args[0]);

        TiDB::TableInfo info;
        // Note: if `table_info_json` is not empty, `table_info` store a ref to `info`
        std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info = std::nullopt;

        Timestamp tombstone = 0;

        if (args.engine_args.size() >= 2)
        {
            const auto * ast = typeid_cast<const ASTLiteral *>(args.engine_args[1].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                const auto table_info_json = safeGet<String>(ast->value);
                if (!table_info_json.empty())
                {
                    info.deserialize(table_info_json);
                    table_info = info;
                }
            }
            else
                throw Exception(
                    "Engine DeltaMerge table info must be a string" + getDeltaMergeVerboseHelp(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
        if (args.engine_args.size() == 3)
        {
            const auto * ast = typeid_cast<const ASTLiteral *>(args.engine_args[2].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
                tombstone = safeGet<UInt64>(ast->value);
            else
                throw Exception(
                    "Engine DeltaMerge tombstone must be a UInt64" + getDeltaMergeVerboseHelp(),
                    ErrorCodes::BAD_ARGUMENTS);
        }
        return StorageDeltaMerge::create(
            args.database_engine,
            args.database_name,
            args.table_name,
            table_info,
            args.columns,
            primary_expr_list,
            tombstone,
            args.context);
    });
}

} // namespace DB

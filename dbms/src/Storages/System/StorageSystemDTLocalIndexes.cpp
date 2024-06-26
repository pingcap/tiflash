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

#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/System/StorageSystemDTLocalIndexes.h>
#include <TiDB/Schema/SchemaNameMapper.h>

namespace DB
{
StorageSystemDTLocalIndexes::StorageSystemDTLocalIndexes(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},

        {"tidb_database", std::make_shared<DataTypeString>()},
        {"tidb_table", std::make_shared<DataTypeString>()},
        {"keyspace_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"table_id", std::make_shared<DataTypeInt64>()},
        {"belonging_table_id", std::make_shared<DataTypeInt64>()},

        {"column_name", std::make_shared<DataTypeString>()},
        {"column_id", std::make_shared<DataTypeUInt64>()},
        {"index_kind", std::make_shared<DataTypeString>()},

        {"rows_stable_indexed", std::make_shared<DataTypeUInt64>()}, // Total rows
        {"rows_stable_not_indexed", std::make_shared<DataTypeUInt64>()}, // Total rows
        {"rows_delta_indexed", std::make_shared<DataTypeUInt64>()}, // Total rows
        {"rows_delta_not_indexed", std::make_shared<DataTypeUInt64>()}, // Total rows
    }));
}

BlockInputStreams StorageSystemDTLocalIndexes::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    SchemaNameMapper mapper;

    auto databases = context.getDatabases();
    for (const auto & d : databases)
    {
        String database_name = d.first;
        const auto & database = d.second;
        const DatabaseTiFlash * db_tiflash = typeid_cast<DatabaseTiFlash *>(database.get());

        auto it = database->getIterator(context);
        for (; it->isValid(); it->next())
        {
            const auto & table_name = it->name();
            auto & storage = it->table();
            if (storage->getName() != MutableSupport::delta_tree_storage_name)
                continue;

            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            const auto & table_info = dm_storage->getTableInfo();
            auto table_id = table_info.id;
            auto store = dm_storage->getStoreIfInited();
            if (!store)
                continue;

            if (dm_storage->isTombstone())
                continue;

            auto index_stats = store->getLocalIndexStats();
            for (auto & stat : index_stats)
            {
                size_t j = 0;
                res_columns[j++]->insert(database_name);
                res_columns[j++]->insert(table_name);

                String tidb_db_name;
                KeyspaceID keyspace_id = NullspaceID;
                if (db_tiflash)
                {
                    tidb_db_name = db_tiflash->getDatabaseInfo().name;
                    keyspace_id = db_tiflash->getDatabaseInfo().keyspace_id;
                }
                res_columns[j++]->insert(tidb_db_name);
                String tidb_table_name = table_info.name;
                res_columns[j++]->insert(tidb_table_name);
                if (keyspace_id == NullspaceID)
                    res_columns[j++]->insert(Field());
                else
                    res_columns[j++]->insert(static_cast<UInt64>(keyspace_id));
                res_columns[j++]->insert(table_id);
                res_columns[j++]->insert(table_info.belonging_table_id);

                res_columns[j++]->insert(stat.column_name);
                res_columns[j++]->insert(stat.column_id);
                res_columns[j++]->insert(stat.index_kind);

                res_columns[j++]->insert(stat.rows_stable_indexed);
                res_columns[j++]->insert(stat.rows_stable_not_indexed);
                res_columns[j++]->insert(stat.rows_delta_indexed);
                res_columns[j++]->insert(stat.rows_delta_not_indexed);
            }
        }
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB

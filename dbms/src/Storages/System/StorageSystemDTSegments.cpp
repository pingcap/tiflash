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
#include <Storages/System/StorageSystemDTSegments.h>
#include <TiDB/Schema/SchemaNameMapper.h>

namespace DB
{
StorageSystemDTSegments::StorageSystemDTSegments(const std::string & name_)
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
        {"is_tombstone", std::make_shared<DataTypeUInt64>()},

        {"segment_id", std::make_shared<DataTypeUInt64>()},
        {"range", std::make_shared<DataTypeString>()},
        {"epoch", std::make_shared<DataTypeUInt64>()},
        {"rows", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},

        {"delta_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_memtable_rows", std::make_shared<DataTypeUInt64>()},
        {"delta_memtable_size", std::make_shared<DataTypeUInt64>()},
        {"delta_memtable_column_files", std::make_shared<DataTypeUInt64>()},
        {"delta_memtable_delete_ranges", std::make_shared<DataTypeUInt64>()},
        {"delta_persisted_page_id", std::make_shared<DataTypeUInt64>()},
        {"delta_persisted_rows", std::make_shared<DataTypeUInt64>()},
        {"delta_persisted_size", std::make_shared<DataTypeUInt64>()},
        {"delta_persisted_column_files", std::make_shared<DataTypeUInt64>()},
        {"delta_persisted_delete_ranges", std::make_shared<DataTypeUInt64>()},
        {"delta_cache_size", std::make_shared<DataTypeUInt64>()},
        {"delta_index_size", std::make_shared<DataTypeUInt64>()},

        {"stable_page_id", std::make_shared<DataTypeUInt64>()},
        {"stable_rows", std::make_shared<DataTypeUInt64>()},
        {"stable_size", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles_id_0", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles_rows", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles_size", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles_size_on_disk", std::make_shared<DataTypeUInt64>()},
        {"stable_dmfiles_packs", std::make_shared<DataTypeUInt64>()},
    }));
}

BlockInputStreams StorageSystemDTSegments::read(
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
            if (storage->getName() != MutSup::delta_tree_storage_name)
                continue;

            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            const auto & table_info = dm_storage->getTableInfo();
            auto table_id = table_info.id;
            auto store = dm_storage->getStoreIfInited();
            if (!store)
                continue;
            auto segment_stats = store->getSegmentsStats();
            for (auto & stat : segment_stats)
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
                res_columns[j++]->insert(dm_storage->getTombstone());

                res_columns[j++]->insert(stat.segment_id);
                res_columns[j++]->insert(stat.range.toString());
                res_columns[j++]->insert(stat.epoch);
                res_columns[j++]->insert(stat.rows);
                res_columns[j++]->insert(stat.size);

                res_columns[j++]->insert(stat.delta_rate);
                res_columns[j++]->insert(stat.delta_memtable_rows);
                res_columns[j++]->insert(stat.delta_memtable_size);
                res_columns[j++]->insert(stat.delta_memtable_column_files);
                res_columns[j++]->insert(stat.delta_memtable_delete_ranges);
                res_columns[j++]->insert(stat.delta_persisted_page_id);
                res_columns[j++]->insert(stat.delta_persisted_rows);
                res_columns[j++]->insert(stat.delta_persisted_size);
                res_columns[j++]->insert(stat.delta_persisted_column_files);
                res_columns[j++]->insert(stat.delta_persisted_delete_ranges);
                res_columns[j++]->insert(stat.delta_cache_size);
                res_columns[j++]->insert(stat.delta_index_size);

                res_columns[j++]->insert(stat.stable_page_id);
                res_columns[j++]->insert(stat.stable_rows);
                res_columns[j++]->insert(stat.stable_size);
                res_columns[j++]->insert(stat.stable_dmfiles);
                res_columns[j++]->insert(stat.stable_dmfiles_id_0);
                res_columns[j++]->insert(stat.stable_dmfiles_rows);
                res_columns[j++]->insert(stat.stable_dmfiles_size);
                res_columns[j++]->insert(stat.stable_dmfiles_size_on_disk);
                res_columns[j++]->insert(stat.stable_dmfiles_packs);
            }
        }
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB

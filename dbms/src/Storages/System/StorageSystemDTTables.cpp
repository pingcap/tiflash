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
#include <Storages/System/StorageSystemDTTables.h>
#include <TiDB/Schema/SchemaNameMapper.h>

namespace DB
{

StorageSystemDTTables::StorageSystemDTTables(const std::string & name_)
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

        {"segment_count", std::make_shared<DataTypeUInt64>()},

        {"total_rows", std::make_shared<DataTypeUInt64>()},
        {"total_size", std::make_shared<DataTypeUInt64>()},
        {"total_delete_ranges", std::make_shared<DataTypeUInt64>()},

        {"delta_rate_rows", std::make_shared<DataTypeFloat64>()},
        {"delta_rate_segments", std::make_shared<DataTypeFloat64>()},

        {"delta_placed_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_size", std::make_shared<DataTypeUInt64>()},
        {"delta_cache_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_wasted_rate", std::make_shared<DataTypeFloat64>()},

        {"delta_index_size", std::make_shared<DataTypeUInt64>()},

        {"avg_segment_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_segment_size", std::make_shared<DataTypeFloat64>()},

        {"delta_count", std::make_shared<DataTypeUInt64>()},
        {"total_delta_rows", std::make_shared<DataTypeUInt64>()},
        {"total_delta_size", std::make_shared<DataTypeUInt64>()},
        {"avg_delta_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_delta_size", std::make_shared<DataTypeFloat64>()},
        {"avg_delta_delete_ranges", std::make_shared<DataTypeFloat64>()},

        {"stable_count", std::make_shared<DataTypeUInt64>()},
        {"total_stable_rows", std::make_shared<DataTypeUInt64>()},
        {"total_stable_size", std::make_shared<DataTypeUInt64>()},
        {"total_stable_size_on_disk", std::make_shared<DataTypeUInt64>()},
        {"avg_stable_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_stable_size", std::make_shared<DataTypeFloat64>()},

        {"total_pack_count_in_delta", std::make_shared<DataTypeUInt64>()},
        {"max_pack_count_in_delta", std::make_shared<DataTypeUInt64>()},
        {"avg_pack_count_in_delta", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_rows_in_delta", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_size_in_delta", std::make_shared<DataTypeFloat64>()},

        {"total_pack_count_in_stable", std::make_shared<DataTypeUInt64>()},
        {"avg_pack_count_in_stable", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_rows_in_stable", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_size_in_stable", std::make_shared<DataTypeFloat64>()},

        {"storage_stable_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_stable_oldest_snapshot_lifetime", std::make_shared<DataTypeFloat64>()},
        {"storage_stable_oldest_snapshot_thread_id", std::make_shared<DataTypeUInt64>()},
        {"storage_stable_oldest_snapshot_tracing_id", std::make_shared<DataTypeString>()},

        {"storage_delta_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_delta_oldest_snapshot_lifetime", std::make_shared<DataTypeFloat64>()},
        {"storage_delta_oldest_snapshot_thread_id", std::make_shared<DataTypeUInt64>()},
        {"storage_delta_oldest_snapshot_tracing_id", std::make_shared<DataTypeString>()},

        {"storage_meta_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_meta_oldest_snapshot_lifetime", std::make_shared<DataTypeFloat64>()},
        {"storage_meta_oldest_snapshot_thread_id", std::make_shared<DataTypeUInt64>()},
        {"storage_meta_oldest_snapshot_tracing_id", std::make_shared<DataTypeString>()},

        {"background_tasks_length", std::make_shared<DataTypeUInt64>()},
    }));
}


BlockInputStreams StorageSystemDTTables::read(
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
            auto stat = store->getStoreStats();
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

            res_columns[j++]->insert(stat.segment_count);

            res_columns[j++]->insert(stat.total_rows);
            res_columns[j++]->insert(stat.total_size);
            res_columns[j++]->insert(stat.total_delete_ranges);

            res_columns[j++]->insert(stat.delta_rate_rows);
            res_columns[j++]->insert(stat.delta_rate_segments);

            res_columns[j++]->insert(stat.delta_placed_rate);
            res_columns[j++]->insert(stat.delta_cache_size);
            res_columns[j++]->insert(stat.delta_cache_rate);
            res_columns[j++]->insert(stat.delta_cache_wasted_rate);

            res_columns[j++]->insert(stat.delta_index_size);

            res_columns[j++]->insert(stat.avg_segment_rows);
            res_columns[j++]->insert(stat.avg_segment_size);

            res_columns[j++]->insert(stat.delta_count);
            res_columns[j++]->insert(stat.total_delta_rows);
            res_columns[j++]->insert(stat.total_delta_size);
            res_columns[j++]->insert(stat.avg_delta_rows);
            res_columns[j++]->insert(stat.avg_delta_size);
            res_columns[j++]->insert(stat.avg_delta_delete_ranges);

            res_columns[j++]->insert(stat.stable_count);
            res_columns[j++]->insert(stat.total_stable_rows);
            res_columns[j++]->insert(stat.total_stable_size);
            res_columns[j++]->insert(stat.total_stable_size_on_disk);
            res_columns[j++]->insert(stat.avg_stable_rows);
            res_columns[j++]->insert(stat.avg_stable_size);

            res_columns[j++]->insert(stat.total_pack_count_in_delta);
            res_columns[j++]->insert(stat.max_pack_count_in_delta);
            res_columns[j++]->insert(stat.avg_pack_count_in_delta);
            res_columns[j++]->insert(stat.avg_pack_rows_in_delta);
            res_columns[j++]->insert(stat.avg_pack_size_in_delta);

            res_columns[j++]->insert(stat.total_pack_count_in_stable);
            res_columns[j++]->insert(stat.avg_pack_count_in_stable);
            res_columns[j++]->insert(stat.avg_pack_rows_in_stable);
            res_columns[j++]->insert(stat.avg_pack_size_in_stable);

            res_columns[j++]->insert(stat.storage_stable_num_snapshots);
            res_columns[j++]->insert(stat.storage_stable_oldest_snapshot_lifetime);
            res_columns[j++]->insert(stat.storage_stable_oldest_snapshot_thread_id);
            res_columns[j++]->insert(stat.storage_stable_oldest_snapshot_tracing_id);

            res_columns[j++]->insert(stat.storage_delta_num_snapshots);
            res_columns[j++]->insert(stat.storage_delta_oldest_snapshot_lifetime);
            res_columns[j++]->insert(stat.storage_delta_oldest_snapshot_thread_id);
            res_columns[j++]->insert(stat.storage_delta_oldest_snapshot_tracing_id);

            res_columns[j++]->insert(stat.storage_meta_num_snapshots);
            res_columns[j++]->insert(stat.storage_meta_oldest_snapshot_lifetime);
            res_columns[j++]->insert(stat.storage_meta_oldest_snapshot_thread_id);
            res_columns[j++]->insert(stat.storage_meta_oldest_snapshot_tracing_id);

            res_columns[j++]->insert(stat.background_tasks_length);
        }
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB

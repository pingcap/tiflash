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

#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/System/StorageSystemDTSegments.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
StorageSystemDTSegments::StorageSystemDTSegments(const std::string & name_) : name(name_)
{
    setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},

        {"tidb_database", std::make_shared<DataTypeString>()},
        {"tidb_table", std::make_shared<DataTypeString>()},
        {"table_id", std::make_shared<DataTypeInt64>()},
        {"is_tombstone", std::make_shared<DataTypeUInt64>()},

        {"segment_id", std::make_shared<DataTypeUInt64>()},
        {"range", std::make_shared<DataTypeString>()},

        {"rows", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"delete_ranges", std::make_shared<DataTypeUInt64>()},

        {"stable_size_on_disk", std::make_shared<DataTypeUInt64>()},

        {"delta_pack_count", std::make_shared<DataTypeUInt64>()},
        {"stable_pack_count", std::make_shared<DataTypeUInt64>()},

        {"avg_delta_pack_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_stable_pack_rows", std::make_shared<DataTypeFloat64>()},

        {"delta_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_size", std::make_shared<DataTypeUInt64>()},
        {"delta_index_size", std::make_shared<DataTypeUInt64>()},
    }));
}

BlockInputStreams StorageSystemDTSegments::read(const Names & column_names,
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
        auto & database = d.second;
        const DatabaseTiFlash * db_tiflash = typeid_cast<DatabaseTiFlash *>(database.get());

        auto it = database->getIterator(context);
        for (; it->isValid(); it->next())
        {
            auto & table_name = it->name();
            auto & storage = it->table();
            if (storage->getName() != MutableSupport::delta_tree_storage_name)
                continue;

            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            auto & table_info = dm_storage->getTableInfo();
            auto table_id = table_info.id;
            auto segment_stats = dm_storage->getStore()->getSegmentStats();
            for (auto & stat : segment_stats)
            {
                size_t j = 0;
                res_columns[j++]->insert(database_name);
                res_columns[j++]->insert(table_name);

                String tidb_db_name;
                if (db_tiflash)
                    tidb_db_name = mapper.displayDatabaseName(db_tiflash->getDatabaseInfo());
                res_columns[j++]->insert(tidb_db_name);
                String tidb_table_name = mapper.displayTableName(table_info);
                res_columns[j++]->insert(tidb_table_name);
                res_columns[j++]->insert(table_id);
                res_columns[j++]->insert(dm_storage->getTombstone());

                res_columns[j++]->insert(stat.segment_id);
                res_columns[j++]->insert(stat.range.toString());
                res_columns[j++]->insert(stat.rows);
                res_columns[j++]->insert(stat.size);
                res_columns[j++]->insert(stat.delete_ranges);

                res_columns[j++]->insert(stat.stable_size_on_disk);

                res_columns[j++]->insert(stat.delta_pack_count);
                res_columns[j++]->insert(stat.stable_pack_count);

                res_columns[j++]->insert(stat.avg_delta_pack_rows);
                res_columns[j++]->insert(stat.avg_stable_pack_rows);

                res_columns[j++]->insert(stat.delta_rate);
                res_columns[j++]->insert(stat.delta_cache_size);

                res_columns[j++]->insert(stat.delta_index_size);
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB

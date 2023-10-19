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
#include <Common/typeid_cast.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/Types.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>


namespace DB
{


StorageSystemDatabases::StorageSystemDatabases(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"name", std::make_shared<DataTypeString>()},
        {"tidb_name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeInt64>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"is_tombstone", std::make_shared<DataTypeUInt64>()},
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
    }));
}


BlockInputStreams StorageSystemDatabases::read(
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
    for (const auto & database : databases)
    {
        size_t j = 0;
        res_columns[j++]->insert(database.first);

        String tidb_db_name;
        DatabaseID database_id = -1;
        Timestamp tombstone = 0;
        if (const DatabaseTiFlash * db_tiflash = typeid_cast<DatabaseTiFlash *>(database.second.get()); db_tiflash)
        {
            auto & db_info = db_tiflash->getDatabaseInfo();
            tidb_db_name = mapper.displayDatabaseName(db_info);
            database_id = db_info.id;
            tombstone = db_tiflash->getTombstone();
        }

        res_columns[j++]->insert(tidb_db_name);
        res_columns[j++]->insert(Int64(database_id));

        res_columns[j++]->insert(database.second->getEngineName());
        res_columns[j++]->insert(static_cast<UInt64>(tombstone));
        res_columns[j++]->insert(database.second->getDataPath());
        res_columns[j++]->insert(database.second->getMetadataPath());
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB

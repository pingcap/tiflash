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

#pragma once

#include <Interpreters/Context.h>
#include <Storages/Transaction/SchemaGetter.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{
template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & databases;

    Int64 target_version;

    Poco::Logger * log;

    SchemaBuilder(Getter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & dbs_, Int64 version)
        : getter(getter_)
        , context(context_)
        , databases(dbs_)
        , target_version(version)
        , log(&Poco::Logger::get("SchemaBuilder"))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

private:
    void applyDropSchema(DatabaseID schema_id);

    /// Parameter db_name should be mapped.
    void applyDropSchema(const String & db_name);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchema(TiDB::DBInfoPtr db_info);

    void applyCreateTable(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyCreateLogicalTable(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info);

    void applyCreatePhysicalTable(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info);

    void applyDropTable(TiDB::DBInfoPtr db_info, TableID table_id);

    /// Parameter schema_name should be mapped.
    void applyDropPhysicalTable(const String & db_name, TableID table_id);

    void applyPartitionDiff(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyPartitionDiff(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info, ManageableStoragePtr storage, bool drop_part_if_not_exist);

    void applyAlterTable(TiDB::DBInfoPtr db_info, TableID table_id);

    void applyAlterLogicalTable(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info, ManageableStoragePtr storage);

    void applyAlterPhysicalTable(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info, ManageableStoragePtr storage);

    void applyRenameTable(TiDB::DBInfoPtr new_db_info, TiDB::TableID table_id);

    void applyRenameLogicalTable(TiDB::DBInfoPtr new_db_info, TiDB::TableInfoPtr new_table_info, ManageableStoragePtr storage);

    void applyRenamePhysicalTable(TiDB::DBInfoPtr new_db_info, TiDB::TableInfo & new_table_info, ManageableStoragePtr storage);

    void applyExchangeTablePartition(const SchemaDiff & diff);

    void applySetTiFlashReplica(TiDB::DBInfoPtr db_info, TableID table_id);
    void applySetTiFlashReplica(TiDB::DBInfoPtr db_info, TiDB::TableInfoPtr table_info, ManageableStoragePtr storage);
};

} // namespace DB

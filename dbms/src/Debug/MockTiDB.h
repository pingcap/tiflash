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

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/SchemaGetter.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDB.h>

#include <atomic>

namespace DB
{
class MockTiDB : public ext::Singleton<MockTiDB>
{
    friend class ext::Singleton<MockTiDB>;

public:
    MockTiDB();
    class Table
    {
        friend class MockTiDB;

    public:
        Table(
            const String & database_name,
            DatabaseID database_id,
            const String & table_name,
            TiDB::TableInfo && table_info);

        TableID id() const { return table_info.id; }
        DatabaseID dbID() const { return database_id; }

        ColumnID allocColumnID() { return ++col_id; }

        bool isPartitionTable() const { return table_info.is_partition_table; }

        std::vector<TableID> getPartitionIDs() const
        {
            std::vector<TableID> partition_ids;
            std::for_each(
                table_info.partition.definitions.begin(),
                table_info.partition.definitions.end(),
                [&](const auto & part_def) { partition_ids.emplace_back(part_def.id); });
            return partition_ids;
        }

        bool existPartitionID(TableID part_id) const
        {
            const auto & part_def = find_if(
                table_info.partition.definitions.begin(),
                table_info.partition.definitions.end(),
                [&part_id](const auto & part_def) { return part_def.id == part_id; });
            return part_def != table_info.partition.definitions.end();
        }

        TiDB::TableInfo table_info;

    private:
        const String database_name;
        DatabaseID database_id;
        const String table_name;
        ColumnID col_id;
    };
    using TablePtr = std::shared_ptr<Table>;

public:
    TableID newTable(
        const String & database_name,
        const String & table_name,
        const ColumnsDescription & columns,
        Timestamp tso,
        const String & handle_pk_name,
        const String & engine_type);

    // Mock to create a partition table with given partition names
    // Return <logical_table_id, [physical_table_id0, physical_table_id1, ...]>
    std::tuple<TableID, std::vector<TableID>> newPartitionTable(
        const String & database_name,
        const String & table_name,
        const ColumnsDescription & columns,
        Timestamp tso,
        const String & handle_pk_name,
        const String & engine_type,
        const Strings & part_names);

    std::vector<TableID> newTables(
        const String & database_name,
        const std::vector<std::tuple<String, ColumnsDescription, String>> & tables,
        Timestamp tso,
        const String & engine_type);

    TableID addTable(const String & database_name, TiDB::TableInfo && table_info);

    static TiDB::TableInfoPtr parseColumns(
        const String & tbl_name,
        const ColumnsDescription & columns,
        const String & handle_pk_name,
        String engine_type);

    DatabaseID newDataBase(const String & database_name);

    TableID newPartition(
        const String & database_name,
        const String & table_name,
        TableID partition_id,
        Timestamp tso,
        bool);
    TableID newPartition(TableID belong_logical_table, const String & partition_name, Timestamp tso, bool);

    void dropPartition(const String & database_name, const String & table_name, TableID partition_id);

    void dropTable(Context & context, const String & database_name, const String & table_name, bool drop_regions);
    void dropTableById(Context & context, const TableID & table_id, bool drop_regions);

    void dropDB(Context & context, const String & database_name, bool drop_regions);

    void addColumnToTable(
        const String & database_name,
        const String & table_name,
        const NameAndTypePair & column,
        const Field & default_value);

    void dropColumnFromTable(const String & database_name, const String & table_name, const String & column_name);

    void modifyColumnInTable(const String & database_name, const String & table_name, const NameAndTypePair & column);

    void renameColumnInTable(
        const String & database_name,
        const String & table_name,
        const String & old_column_name,
        const String & new_column_name);

    void renameTable(const String & database_name, const String & table_name, const String & new_table_name);
    // Rename table to another database
    void renameTableTo(
        const String & database_name,
        const String & table_name,
        const String & new_database_name,
        const String & new_table_name);

    void renameTables(const std::vector<std::tuple<std::string, std::string, std::string>> & table_name_map);

    void truncateTable(const String & database_name, const String & table_name);

    // Mock that concurrent DDL meets conflict, it will retry with a new schema version
    // Return the schema_version with empty SchemaDiff
    Int64 skipSchemaVersion() { return ++version; }

    Int64 regenerateSchemaMap();

    TablePtr getTableByName(const String & database_name, const String & table_name);

    TiDB::TableInfoPtr getTableInfoByID(TableID table_id);

    TiDB::DBInfoPtr getDBInfoByID(DatabaseID db_id);

    std::pair<bool, DatabaseID> getDBIDByName(const String & database_name);

    bool checkSchemaDiffExists(Int64 version);

    std::optional<SchemaDiff> getSchemaDiff(Int64 version);

    std::unordered_map<String, DatabaseID> getDatabases() { return databases; }

    std::unordered_map<TableID, TablePtr> getTables() { return tables_by_id; }

    Int64 getVersion() const { return version; }

    TableID newTableID() { return table_id_allocator++; }

private:
    TableID newPartitionImpl(
        const TablePtr & logical_table,
        TableID partition_id,
        const String & partition_name,
        Timestamp tso,
        bool is_add_part);
    TablePtr dropTableByNameImpl(
        Context & context,
        const String & database_name,
        const String & table_name,
        bool drop_regions);
    TablePtr dropTableByIdImpl(Context & context, TableID table_id, bool drop_regions);
    TablePtr dropTableInternal(Context & context, const TablePtr & table, bool drop_regions);
    TablePtr getTableByNameInternal(const String & database_name, const String & table_name);
    TablePtr getTableByID(TableID table_id);

private:
    std::mutex tables_mutex;

    std::unordered_map<String, DatabaseID> databases;
    std::unordered_map<String, TablePtr> tables_by_name;
    std::unordered_map<TableID, TablePtr> tables_by_id;

    std::unordered_map<Int64, SchemaDiff> version_diff;

    std::atomic<TableID> table_id_allocator = 30;

    Int64 version = 0;
};

Field getDefaultValue(const ASTPtr & default_value_ast);

} // namespace DB

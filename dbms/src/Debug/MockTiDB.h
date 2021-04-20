#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/Transaction/SchemaGetter.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

#include <atomic>

namespace DB
{

class MockTiDB : public ext::singleton<MockTiDB>
{
    friend class ext::singleton<MockTiDB>;

public:
    MockTiDB();
    class Table
    {
        friend class MockTiDB;

    public:
        Table(const String & database_name, DatabaseID database_id, const String & table_name, TiDB::TableInfo && table_info);

        TableID id() { return table_info.id; }
        DatabaseID dbID() { return database_id; }

        ColumnID allocColumnID() { return ++col_id; }

        bool isPartitionTable() { return table_info.is_partition_table; }

        std::vector<TableID> getPartitionIDs()
        {
            std::vector<TableID> partition_ids;
            std::for_each(table_info.partition.definitions.begin(), table_info.partition.definitions.end(),
                [&](const TiDB::PartitionDefinition & part_def) { partition_ids.emplace_back(part_def.id); });
            return partition_ids;
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
    TableID newTable(const String & database_name, const String & table_name, const ColumnsDescription & columns, Timestamp tso,
        const String & handle_pk_name, const String & engine_type);

    static TiDB::TableInfoPtr parseColumns(
        const String & tbl_name, const ColumnsDescription & columns, const String & handle_pk_name, String engine_type);

    DatabaseID newDataBase(const String & database_name);

    void newPartition(const String & database_name, const String & table_name, TableID partition_id, Timestamp tso, bool);

    void dropPartition(const String & database_name, const String & table_name, TableID partition_id);

    void dropTable(Context & context, const String & database_name, const String & table_name, bool drop_regions);

    void dropDB(Context & context, const String & database_name, bool drop_regions);

    void addColumnToTable(
        const String & database_name, const String & table_name, const NameAndTypePair & column, const Field & default_value);

    void dropColumnFromTable(const String & database_name, const String & table_name, const String & column_name);

    void modifyColumnInTable(const String & database_name, const String & table_name, const NameAndTypePair & column);

    void renameColumnInTable(
        const String & database_name, const String & table_name, const String & old_column_name, const String & new_column_name);

    void renameTable(const String & database_name, const String & table_name, const String & new_table_name);

    void truncateTable(const String & database_name, const String & table_name);

    TablePtr getTableByName(const String & database_name, const String & table_name);

    TiDB::TableInfoPtr getTableInfoByID(TableID table_id);

    TiDB::DBInfoPtr getDBInfoByID(DatabaseID db_id);

    SchemaDiff getSchemaDiff(Int64 version);

    std::unordered_map<String, DatabaseID> getDatabases() { return databases; }

    std::unordered_map<TableID, TablePtr> getTables() { return tables_by_id; }

    Int64 getVersion() { return version; }

private:
    TablePtr dropTableInternal(Context & context, const String & database_name, const String & table_name, bool drop_regions);
    TablePtr getTableByNameInternal(const String & database_name, const String & table_name);

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

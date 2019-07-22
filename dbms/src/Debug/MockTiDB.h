#pragma once

#include <atomic>

#include <Storages/ColumnsDescription.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

class MockTiDB : public ext::singleton<MockTiDB>
{
    friend class ext::singleton<MockTiDB>;

public:
    class Table
    {
        friend class MockTiDB;

    public:
        Table(const String & database_name, const String & table_name, TiDB::TableInfo && table_info);

        TableID id() { return table_info.id; }

        bool isPartitionTable() { return table_info.is_partition_table; }

        TableID getPartitionIDByName(const String & partition_name)
        {
            const auto & partition_def = std::find_if(table_info.partition.definitions.begin(), table_info.partition.definitions.end(),
                [&partition_name](const TiDB::PartitionDefinition & part_def) { return part_def.name == partition_name; });

            if (partition_def == table_info.partition.definitions.end())
                throw Exception("Mock TiDB table " + database_name + "." + table_name + " does not have partition " + partition_name,
                    ErrorCodes::LOGICAL_ERROR);

            return partition_def->id;
        }

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
        const String table_name;
    };
    using TablePtr = std::shared_ptr<Table>;

public:
    TableID newTable(const String & database_name, const String & table_name, const ColumnsDescription & columns, Timestamp tso);

    TableID newPartition(const String & database_name, const String & table_name, const String & partition_name, Timestamp tso);

    void dropTable(const String & database_name, const String & table_name);

    void addColumnToTable(const String & database_name, const String & table_name, const NameAndTypePair & column);

    void dropColumnFromTable(const String & database_name, const String & table_name, const String & column_name);

    void modifyColumnInTable(const String & database_name, const String & table_name, const NameAndTypePair & column);

    void renameTable(const String & database_name, const String & table_name, const String & new_table_name);

    void truncateTable(const String & database_name, const String & table_name);

    TablePtr getTableByName(const String & database_name, const String & table_name);

    void traverseTables(std::function<void(TablePtr)> f);

private:
    TablePtr getTableByNameInternal(const String & database_name, const String & table_name);

private:
    std::mutex tables_mutex;

    std::unordered_map<String, DatabaseID> databases;
    std::unordered_map<String, TablePtr> tables_by_name;
    std::unordered_map<TableID, TablePtr> tables_by_id;
};

} // namespace DB

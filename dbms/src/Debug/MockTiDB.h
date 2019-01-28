#pragma once

#include <atomic>

#include <Storages/ColumnsDescription.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

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

        const TiDB::TableInfo table_info;

    private:
        const String          database_name;
        const String          table_name;
    };
    using TablePtr = std::shared_ptr<Table>;

    class MockSchemaSyncer : public JsonSchemaSyncer
    {
    protected:
        String getSchemaJson(TableID table_id, Context & /*context*/) override
        {
            return MockTiDB::instance().getSchemaJson(table_id);
        }
    };

public:
    String getSchemaJson(TableID table_id);

    TableID newTable(const String & database_name, const String & table_name, const ColumnsDescription & columns);

    void dropTable(const String & database_name, const String & table_name);

    TablePtr getTableByName(const String & database_name, const String & table_name);

private:
    std::mutex tables_mutex;

    std::unordered_map<String, DatabaseID> databases;
    std::unordered_map<String, TablePtr>   tables_by_name;
    std::unordered_map<TableID, TablePtr>  tables_by_id;
};

} // namespace DB

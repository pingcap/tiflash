#pragma once

#include <Debug/MockTiDB.h>

namespace DB
{

class MockSchemaSyncer : public SchemaSyncer
{
public:
    MockSchemaSyncer();

    bool syncSchemas(Context & context) override;

    void syncSchema(Context & context, TableID table_id, bool lock = true) override;

    TableID getTableIdByName(const std::string & database_name, const std::string & table_name)
    {
        return MockTiDB::instance().getTableIDByName(database_name, table_name);
    }

protected:
    String getSchemaJson(TableID table_id, Context & /*context*/) { return MockTiDB::instance().getSchemaJson(table_id); }

    Logger * log;
};

} // namespace DB

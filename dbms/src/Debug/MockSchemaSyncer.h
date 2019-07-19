#pragma once

#include <Debug/MockTiDB.h>

#include <unordered_map>

namespace DB
{

class MockSchemaSyncer : public SchemaSyncer
{
public:
    MockSchemaSyncer();

    bool syncSchemas(Context & context) override;

protected:
    void syncTable(Context & context, MockTiDB::TablePtr table);

    Logger * log;

    std::mutex schema_mutex;

    std::unordered_map<TableID, MockTiDB::TablePtr> tables;
};

} // namespace DB

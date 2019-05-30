#pragma once

#include <common/logger_useful.h>

#include <Interpreters/Context.h>
#include <Storages/Transaction/Types.h>


namespace DB
{

class SchemaSyncer
{
public:
    virtual ~SchemaSyncer() = default;

    /**
     * Synchronize schema between TiDB and CH, to make sure the CH table is new enough to accept data from raft.
     * Should be stateless.
     * Nevertheless, the implementations may assume that the storage is appropriately locked, thus still not thread-safe.
     * @param table_id
     * @param context
     */
    virtual void syncSchema(TableID table_id, Context & context, bool force) = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

/// Schema syncer implementation using schema described as Json, provided by TiDB or TiKV.
class JsonSchemaSyncer : public SchemaSyncer
{
public:
    JsonSchemaSyncer();

    void syncSchema(TableID table_id, Context & context, bool force) override;

protected:
    virtual String getSchemaJson(TableID table_id, Context & context) = 0;

protected:
    std::unordered_set<TableID> ignored_tables;

    Logger * log;
};

/// Json-based schema syncer implementation fetching schema Json from TiDB via HTTP.
class HttpJsonSchemaSyncer : public JsonSchemaSyncer
{
protected:
    String getSchemaJson(TableID table_id, Context & context) override;
};

} // namespace DB
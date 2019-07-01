#pragma once

#include <common/logger_useful.h>

#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/Types.h>


namespace DB
{

class SchemaSyncer
{
public:
    virtual ~SchemaSyncer() = default;

    /**
     * Synchronize all schemas between TiDB and CH.
     * @param context
     */
    virtual bool syncSchemas(Context & context) = 0;

    /**
     * Synchronize schema between TiDB and CH, to make sure the CH table is new enough to accept data from raft.
     * Should be stateless.
     * Nevertheless, the implementations may assume that the storage is appropriately locked, thus still not thread-safe.
     * @param table_id
     * @param context
     */
    //virtual void syncSchema(TableID table_id, Context & context, bool force) = 0;

//    virtual TableID getTableIdByName(const std::string & database_name, const std::string & table_name, Context & context) = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

/// Schema syncer implementation using schema described as Json, provided by TiDB or TiKV.
class JsonSchemaSyncer : public SchemaSyncer
{
public:
    JsonSchemaSyncer();

    bool syncSchemas(Context & context) override;

    void syncSchema(TableID table_id, Context & context, bool force);

    TableID getTableIdByName(const std::string & database_name, const std::string & table_name, Context & context);

protected:
    virtual String getSchemaJson(TableID table_id, Context & context) = 0;
    virtual String getSchemaJsonByName(const std::string & database_name, const std::string & table_name, Context & context) = 0;

protected:
    std::unordered_set<TableID> ignored_tables;

    Logger * log;
};

/// Json-based schema syncer implementation fetching schema Json from TiDB via HTTP.
class HttpJsonSchemaSyncer : public JsonSchemaSyncer
{
protected:
    String getSchemaJson(TableID table_id, Context & context) override;
    String getSchemaJsonByName(const std::string & database_name, const std::string & table_name, Context & context) override;
};

} // namespace DB

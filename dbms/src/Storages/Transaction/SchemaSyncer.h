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
     * @param context
     * @param table_id
     */
    virtual void syncSchema(Context & context, TableID table_id, bool lock = true) = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

} // namespace DB

#pragma once

#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>


namespace DB
{

class SchemaSyncer
{
public:
    virtual ~SchemaSyncer() = default;

    /**
     * Get current version of CH schema.
     */
    virtual Int64 getCurrentVersion() = 0;

    /**
     * Synchronize all schemas between TiDB and CH.
     * @param context
     */
    virtual bool syncSchemas(Context & context) = 0;

    virtual void reset() = 0;

    virtual TiDB::DBInfoPtr getDBInfoByName(const String & database_name) = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

} // namespace DB

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
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

} // namespace DB

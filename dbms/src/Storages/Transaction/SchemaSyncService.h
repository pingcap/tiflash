#pragma once

#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

class Context;
class BackgroundProcessingPool;

class SchemaSyncService : public std::enable_shared_from_this<SchemaSyncService>, private boost::noncopyable
{
public:
    SchemaSyncService(Context & context_);
    ~SchemaSyncService();

private:
    bool syncSchemas();

    struct GCContext
    {
        Timestamp last_gc_safe_point = 0;
    } gc_context;

    bool gc(Timestamp gc_safe_point);

private:
    Context & context;

    BackgroundProcessingPool & background_pool;
    BackgroundProcessingPool::TaskHandle handle;

    Logger * log;
};

using SchemaSyncServicePtr = std::shared_ptr<SchemaSyncService>;

} // namespace DB

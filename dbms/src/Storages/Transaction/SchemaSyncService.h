#pragma once

#include <memory>

#include <common/logger_useful.h>
#include <boost/noncopyable.hpp>

#include <Storages/MergeTree/BackgroundProcessingPool.h>

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
    Context & context;

    BackgroundProcessingPool & background_pool;
    BackgroundProcessingPool::TaskHandle handle;

    Logger * log;
};

} // namespace DB

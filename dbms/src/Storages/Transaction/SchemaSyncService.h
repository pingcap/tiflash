#pragma once

#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/Transaction/Types.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace Poco
{
class Logger;
}

namespace DB
{

class Context;
class BackgroundProcessingPool;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;
using DBGInvokerPrinter = std::function<void(const std::string &)>;
extern void dbgFuncGcSchemas(Context &, const ASTs &, DBGInvokerPrinter);

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

    friend void dbgFuncGcSchemas(Context &, const ASTs &, DBGInvokerPrinter);

    BackgroundProcessingPool & background_pool;
    BackgroundProcessingPool::TaskHandle handle;

    Poco::Logger * log;
};

using SchemaSyncServicePtr = std::shared_ptr<SchemaSyncService>;

} // namespace DB

#include <Interpreters/Context.h>
#include <Storages/Transaction/SchemaSyncService.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

SchemaSyncService::SchemaSyncService(DB::Context & context_)
    : context(context_), background_pool(context_.getBackgroundPool()), log(&Logger::get("SchemaSyncService"))
{
    handle = background_pool.addTask([&, this] {
        bool succeed = true;
        try
        {
            context.getTMTContext().getSchemaSyncer()->syncSchemas(context);
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(log, "Schema sync failed by " << e.what());
            succeed = false;
        }
        return succeed;
    });
}

SchemaSyncService::~SchemaSyncService() { background_pool.removeTask(handle); }

} // namespace DB

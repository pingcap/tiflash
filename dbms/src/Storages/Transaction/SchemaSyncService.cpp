#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/SchemaSyncService.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

SchemaSyncService::SchemaSyncService(DB::Context & context_)
    : context(context_), background_pool(context_.getBackgroundPool()), log(&Logger::get("SchemaSyncService"))
{
    handle = background_pool.addTask(
        [&, this] {
            String stage;
            try
            {
                /// Do sync schema first, then gc.
                /// They must be performed synchronously,
                /// otherwise table may get mis-GC-ed if RECOVER was not properly synced caused by schema sync pause but GC runs too aggressively.
                stage = "Sync schemas";
                if (!syncSchemas())
                    return false;

                stage = "GC";
                return gc();
            }
            catch (const Exception & e)
            {
                LOG_ERROR(log,
                    __PRETTY_FUNCTION__ << ": " << stage << " failed by " << e.displayText()
                                        << " \n stack : " << e.getStackTrace().toString());
            }
            catch (const Poco::Exception & e)
            {
                LOG_ERROR(log, __PRETTY_FUNCTION__ << ": " << stage << " failed by " << e.displayText());
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(log, __PRETTY_FUNCTION__ << ": " << stage << " failed by " << e.what());
            }
            return false;
        },
        false);
}

SchemaSyncService::~SchemaSyncService() { background_pool.removeTask(handle); }

bool SchemaSyncService::syncSchemas() { return context.getTMTContext().getSchemaSyncer()->syncSchemas(context); }

bool SchemaSyncService::gc()
{
    auto & tmt_context = context.getTMTContext();
    auto pd_client = tmt_context.getPDClient();
    auto gc_safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client);
    if (gc_safe_point == gc_context.last_gc_safe_point)
        return false;

    for (auto & pair : tmt_context.getStorages().getAllStorage())
    {
        auto storage = pair.second;
        if (!storage->isTombstone() || storage->getTombstone() >= gc_safe_point)
            continue;

        String database_name = storage->getDatabaseName();
        String table_name = storage->getDatabaseName();
        auto db_info = tmt_context.getSchemaSyncer()->getDBInfoByName(database_name);
        const auto & table_info = storage->getTableInfo();
        auto canonical_name = SchemaNameMapper().displayCanonicalName(*db_info, table_info);
        LOG_INFO(log, "Physically dropping table " << canonical_name);
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = std::move(database_name);
        drop_query->table = std::move(table_name);
        drop_query->if_exists = true;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, context);
        drop_interpreter.execute();
        LOG_INFO(log, "Physically dropped table " << canonical_name);
    }

    gc_context.last_gc_safe_point = gc_safe_point;
    return true;
}

} // namespace DB

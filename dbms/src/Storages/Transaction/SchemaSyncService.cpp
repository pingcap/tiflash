#include <Common/TiFlashMetrics.h>
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
            bool done_anything = false;
            try
            {
                /// Do sync schema first, then gc.
                /// They must be performed synchronously,
                /// otherwise table may get mis-GC-ed if RECOVER was not properly synced caused by schema sync pause but GC runs too aggressively.
                // GC safe point must be obtained ahead of syncing schema.
                auto gc_safe_point = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient());
                stage = "Sync schemas";
                done_anything = syncSchemas();
                if (done_anything)
                    GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_trigger_count, type_timer).Increment();

                stage = "GC";
                done_anything = gc(gc_safe_point);

                return done_anything;
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

bool SchemaSyncService::gc(Timestamp gc_safe_point)
{
    auto & tmt_context = context.getTMTContext();
    auto pd_client = tmt_context.getPDClient();
    if (gc_safe_point == gc_context.last_gc_safe_point)
        return false;

    LOG_DEBUG(log, "Performing GC using safe point " << gc_safe_point);

    for (auto & pair : tmt_context.getStorages().getAllStorage())
    {
        auto storage = pair.second;
        if (!storage->isTombstone() || storage->getTombstone() >= gc_safe_point)
            continue;

        String database_name = storage->getDatabaseName();
        String table_name = storage->getTableName();
        const auto & table_info = storage->getTableInfo();
        auto canonical_name = [&]() {
            // DB info maintenance is parallel with GC logic so we can't always assume one specific DB info's existence, thus checking its validity.
            auto db_info = tmt_context.getSchemaSyncer()->getDBInfoByMappedName(database_name);
            return db_info ? SchemaNameMapper().debugCanonicalName(*db_info, table_info)
                           : database_name + "." + SchemaNameMapper().debugTableName(table_info);
        }();
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

    LOG_DEBUG(log, "Performed GC using safe point " << gc_safe_point);

    return true;
}

} // namespace DB

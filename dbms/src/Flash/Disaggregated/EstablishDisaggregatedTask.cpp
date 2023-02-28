#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Disaggregated/EstablishDisaggregatedTask.h>
#include <Flash/Executor/QueryExecutorHolder.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int REGION_EPOCH_NOT_MATCH;
} // namespace ErrorCodes

EstablishDisaggregatedTask::EstablishDisaggregatedTask(ContextPtr context_)
    : context(std::move(context_))
    , log(Logger::get("EstablishDisaggregatedTask")) // TODO: add id
{}

// Some preparation
// - Parse the encoded plan
// - Build `dag_context`
// - Set the read_tso, schema_version, timezone
// - Register the task
void EstablishDisaggregatedTask::prepare(const mpp::EstablishDisaggregatedTaskRequest * const request)
{
    auto & tmt_context = context->getTMTContext();
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(request->regions(), request->table_regions(), tmt_context);
    LOG_DEBUG(log, "EstablishDisaggregatedTask handling {} regions from {} physical tables", tables_regions_info.regionCount(), tables_regions_info.tableCount());

    // set schema ver and start ts // TODO: set timeout
    auto schema_ver = request->schema_ver();
    context->setSetting("schema_version", schema_ver);
    const auto & meta = request->meta();
    auto start_ts = meta.start_ts();
    context->setSetting("read_tso", start_ts);

    // Parse the encoded plan into `dag_req`
    dag_req = getDAGRequestFromStringWithRetry(request->encoded_plan());
    LOG_DEBUG(log, "DAGReq: {}", dag_req.ShortDebugString());

    context->getTimezoneInfo().resetByDAGRequest(dag_req);

    DM::DisaggregatedTaskId task_id(meta);

    dag_context = std::make_unique<DAGContext>(
        dag_req,
        task_id,
        std::move(tables_regions_info),
        context->getClientInfo().current_address.toString(),
        Logger::get(fmt::format("{}", task_id)));
    context->setDAGContext(dag_context.get());
}

void EstablishDisaggregatedTask::execute(mpp::EstablishDisaggregatedTaskResponse * response)
{
    query_executor_holder.set(queryExecute(*context));

    auto & tmt = context->getTMTContext();
    {
        auto kvstore = tmt.getKVStore();
        auto store_meta = kvstore->getStoreMeta();
        response->set_store_id(store_meta.id());
    }

    auto * manager = tmt.getDisaggregatedSnapshotManager();
    const auto & task_id = *dag_context->getDisaggregatedTaskId();
    auto snap = manager->getSnapshot(task_id);
    if (!snap)
        throw Exception(fmt::format("Snapshot for {} was missing", task_id));

    for (const auto & [table_id, table_tasks] : snap->tableSnapshots())
    {
        response->add_tables(table_tasks->toRemote(task_id).SerializeAsString());
    }
}

} // namespace DB

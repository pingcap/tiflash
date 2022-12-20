#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Disaggregated/DisaggregatedTask.h>
#include <Flash/Executor/QueryExecutorHolder.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>

#include "Flash/Mpp/MPPTaskId.h"
#include "Storages/DeltaMerge/Remote/DisaggregatedTaskId.h"

namespace DB
{
DisaggregatedTask::DisaggregatedTask(ContextPtr context_)
    : context(std::move(context_))
    , log(Logger::get("DisaggregatedTask")) // TODO: add id
{}

// Some preparation
// - Parse the encoded plan
// - Build `dag_context`
// - Set the read_tso, schema_version, timezone
// - Register the task
void DisaggregatedTask::prepare(const mpp::EstablishDisaggregatedTaskRequest * const request)
{
    auto & tmt_context = context->getTMTContext();
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(request->regions(), request->table_regions(), tmt_context);
    LOG_DEBUG(log, "Handling {} regions from {} physical tables in Disaggregrated task", tables_regions_info.regionCount(), tables_regions_info.tableCount());

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

    MPPTaskId mpp_task_id(meta.start_ts(), meta.task_id(), -1, meta.query_ts(), meta.local_query_id());
    DM::DisaggregatedTaskId task_id(mpp_task_id, meta.executor_id());

    dag_context = std::make_unique<DAGContext>(
        dag_req,
        task_id,
        std::move(tables_regions_info),
        context->getClientInfo().current_address.toString(),
        Logger::get("DisaggregatedTaskHandler"));
    context->setDAGContext(dag_context.get());

    // TODO: register task
}

void DisaggregatedTask::execute()
{
    query_executor_holder.set(queryExecute(*context));
}

} // namespace DB

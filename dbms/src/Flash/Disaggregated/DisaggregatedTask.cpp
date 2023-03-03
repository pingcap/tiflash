// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Disaggregated/DisaggregatedTask.h>
#include <Flash/Executor/QueryExecutorHolder.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshotManager.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include "disaggregated.pb.h"

namespace DB
{

namespace ErrorCodes
{
extern const int REGION_EPOCH_NOT_MATCH;
} // namespace ErrorCodes

DisaggregatedTask::DisaggregatedTask(ContextPtr context_, const DM::DisaggregatedTaskId & task_id)
    : context(std::move(context_))
    , log(Logger::get(fmt::format("{}", task_id)))
{}

// Some preparation
// - Parse the encoded plan
// - Build `dag_context`
// - Set the read_tso, schema_version, timezone
void DisaggregatedTask::prepare(const disaggregated::EstablishDisaggregatedTaskRequest * request)
{
    const auto & meta = request->meta();
    DM::DisaggregatedTaskId task_id(meta);
    auto task = std::make_shared<DisaggregatedTask>(context, task_id);

    auto & tmt_context = context->getTMTContext();
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(request->regions(), request->table_regions(), tmt_context);
    LOG_DEBUG(task->log, "DisaggregatedTask handling {} regions from {} physical tables", tables_regions_info.regionCount(), tables_regions_info.tableCount());

    // set schema ver and start ts // TODO: set timeout
    auto schema_ver = request->schema_ver();
    context->setSetting("schema_version", schema_ver);
    auto start_ts = meta.start_ts();
    context->setSetting("read_tso", start_ts);

    // Parse the encoded plan into `dag_req`
    task->dag_req = getDAGRequestFromStringWithRetry(request->encoded_plan());
    LOG_DEBUG(task->log, "DAGReq: {}", task->dag_req.ShortDebugString());

    context->getTimezoneInfo().resetByDAGRequest(task->dag_req);

    task->dag_context = std::make_unique<DAGContext>(
        task->dag_req,
        task_id,
        std::move(tables_regions_info),
        context->getClientInfo().current_address.toString(),
        task->log);
    context->setDAGContext(task->dag_context.get());
}

void DisaggregatedTask::execute(disaggregated::EstablishDisaggregatedTaskResponse * response)
{
    // run into DAGStorageInterpreter and build the segment snapshots
    query_executor_holder.set(queryExecute(*context));

    auto & tmt = context->getTMTContext();
    {
        const auto & kvstore = tmt.getKVStore();
        response->set_store_id(kvstore->getStoreID());
    }

    auto * manager = tmt.getDisaggregatedSnapshotManager();
    const auto & task_id = *dag_context->getDisaggregatedTaskId();
    auto snap = manager->getSnapshot(task_id);
    if (!snap)
        throw Exception(fmt::format("Snapshot for {} was missing", task_id));

    using DM::Remote::Serializer;
    for (const auto & [table_id, table_tasks] : snap->tableSnapshots())
    {
        response->add_tables(Serializer::serializeTo(table_tasks, task_id).SerializeAsString());
    }
}

} // namespace DB

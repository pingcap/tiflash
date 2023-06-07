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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Disaggregated/WNEstablishDisaggTaskHandler.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <kvproto/disaggregated.pb.h>

namespace DB
{

WNEstablishDisaggTaskHandler::WNEstablishDisaggTaskHandler(ContextPtr context_, const DM::DisaggTaskId & task_id)
    : context(std::move(context_))
    , log(Logger::get(task_id))
{}

// Some preparation
// - Parse the encoded plan
// - Build `dag_context`
// - Set the read_tso, schema_version, timezone
void WNEstablishDisaggTaskHandler::prepare(const disaggregated::EstablishDisaggTaskRequest * request)
{
    const auto & meta = request->meta();

    auto & tmt_context = context->getTMTContext();
    TablesRegionsInfo tables_regions_info = TablesRegionsInfo::create(request->regions(), request->table_regions(), tmt_context);
    LOG_DEBUG(log, "DisaggregatedTask handling {} regions from {} physical tables", tables_regions_info.regionCount(), tables_regions_info.tableCount());

    // set schema ver and start ts
    auto schema_ver = request->schema_ver();
    context->setSetting("schema_version", schema_ver);
    auto start_ts = meta.start_ts();
    context->setSetting("read_tso", start_ts);

    if (request->timeout_s() < 0)
    {
        throw TiFlashException(Errors::Coprocessor::BadRequest, "invalid timeout={}", request->timeout_s());
    }
    else if (request->timeout_s() > 0)
    {
        context->setSetting("disagg_task_snapshot_timeout", request->timeout_s());
    } // use default timeout if it is 0

    // Parse the encoded plan into `dag_req`
    dag_req = getDAGRequestFromStringWithRetry(request->encoded_plan());
    LOG_DEBUG(log, "DAGReq: {}", dag_req.ShortDebugString());

    context->getTimezoneInfo().resetByDAGRequest(dag_req);

    dag_context = std::make_unique<DAGContext>(
        dag_req,
        meta,
        std::move(tables_regions_info),
        context->getClientInfo().current_address.toString(),
        log);
    context->setDAGContext(dag_context.get());
}

void WNEstablishDisaggTaskHandler::execute(disaggregated::EstablishDisaggTaskResponse * response)
{
    // Set the store_id to response before executing query
    auto & tmt = context->getTMTContext();
    {
        const auto & kvstore = tmt.getKVStore();
        response->set_store_id(kvstore->getStoreID());
    }

    // run into DAGStorageInterpreter and build the segment snapshots
    query_executor_holder.set(queryExecute(*context));

    auto snaps = context->getSharedContextDisagg()->wn_snapshot_manager;
    const auto & task_id = *dag_context->getDisaggTaskId();
    auto snap = snaps->getSnapshot(task_id);
    RUNTIME_CHECK_MSG(snap, "Snapshot was missing, task_id={}", task_id);

    {
        auto snapshot_id = task_id.toMeta();
        response->mutable_snapshot_id()->Swap(&snapshot_id);
    }

    using DM::Remote::Serializer;
    snap->iterateTableSnapshots([&](const DM::Remote::DisaggPhysicalTableReadSnapshotPtr & snap) {
        response->add_tables(Serializer::serializeTo(snap, task_id).SerializeAsString());
    });
}

} // namespace DB

// Copyright 2025 PingCAP, Inc.
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

#include <Core/Names.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Flash/Coprocessor/StorageTantivyInterpreter.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <Interpreters/Context.h>
#include <Operators/CoprocessorReaderSourceOp.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTantivy.h>


namespace DB
{

void StorageTantivyIterpreter::execute(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder)
{
    storage->splitRemoteReadAndLocalRead();
    // local_read
    storage->read(exec_context, group_builder, Names(), SelectQueryInfo(), context, 0, max_streams);
    // remote_read
    auto remote_shard_infos = storage->getRemoteShardInfos();
    if (!remote_shard_infos.empty())
    {
        if (context.getDAGContext()->isCop())
            throw RegionException({}, RegionException::RegionReadStatus::NOT_FOUND, "shard not found");
        context.getDAGContext()->retry_shards.insert(
            context.getDAGContext()->retry_shards.end(),
            remote_shard_infos.begin(),
            remote_shard_infos.end());
    }

    auto remote_request = buildRemoteRequests(remote_shard_infos);
    if (!remote_request.empty())
    {
        buildRemoteExec(exec_context, group_builder, remote_request);
    }
}

std::vector<RemoteRequest> StorageTantivyIterpreter::buildRemoteRequests(ShardInfoList & remote_shard_infos)
{
    std::vector<RemoteRequest> remote_requests;
    if (remote_shard_infos.empty())
    {
        return remote_requests;
    }
    remote_requests.push_back(RemoteRequest::build(
        remote_shard_infos,
        *context.getDAGContext(),
        tici_scan,
        context.getDAGContext()->getConnectionID(),
        context.getDAGContext()->getConnectionAlias(),
        log));
    return remote_requests;
}

void StorageTantivyIterpreter::buildRemoteExec(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const std::vector<RemoteRequest> & remote_requests)
{
    auto coprocessor_reader = buildCoprocessorReader(remote_requests);
    size_t concurrent_num = coprocessor_reader->enableCopStream() ? context.getSettingsRef().max_threads.get()
                                                                  : coprocessor_reader->getConcurrency();

    for (size_t i = 0; i < concurrent_num; ++i)
        group_builder.addConcurrency(
            std::make_unique<CoprocessorReaderSourceOp>(exec_context, log->identifier(), coprocessor_reader));

    LOG_DEBUG(log, "remote sourceOps built");
}

CoprocessorReaderPtr StorageTantivyIterpreter::buildCoprocessorReader(
    const std::vector<RemoteRequest> & remote_requests)
{
    std::vector<pingcap::coprocessor::CopTask> all_tasks = buildCopTasks(remote_requests);
    if (all_tasks.empty())
    {
        throw TiFlashException(
            "No coprocessor tasks built for remote read, please check remote request",
            Errors::Coprocessor::BadRequest);
    }
    const DAGSchema & schema = remote_requests[0].schema;
    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    bool has_enforce_encode_type = true;
    pingcap::kv::LabelFilter tiflash_label_filter = pingcap::kv::labelFilterNoTiFlashWriteNode;

    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t queue_size = context.getSettingsRef().remote_read_queue_size > 0
        ? context.getSettingsRef().remote_read_queue_size.get()
        : concurrent_num * 4;
    bool enable_cop_stream = context.getSettingsRef().enable_cop_stream_for_remote_read;
    UInt64 cop_timeout = context.getSettingsRef().cop_timeout_for_remote_read;
    String store_zone_label;
    auto kv_store = tmt.getKVStore();
    if likely (kv_store)
    {
        for (int i = 0; i < kv_store->getStoreMeta().labels_size(); ++i)
        {
            if (kv_store->getStoreMeta().labels().at(i).key() == "zone")
            {
                store_zone_label = kv_store->getStoreMeta().labels().at(i).value();
                break;
            }
        }
    }
    auto coprocessor_reader = std::make_shared<CoprocessorReader>(
        schema,
        cluster,
        std::move(all_tasks),
        has_enforce_encode_type,
        concurrent_num,
        enable_cop_stream,
        queue_size,
        cop_timeout,
        tiflash_label_filter,
        log->identifier(),
        store_zone_label);
    context.getDAGContext()->addCoprocessorReader(coprocessor_reader);

    return coprocessor_reader;
}

std::vector<pingcap::coprocessor::CopTask> StorageTantivyIterpreter::buildCopTasks(
    const std::vector<RemoteRequest> & remote_requests)
{
    std::vector<pingcap::coprocessor::CopTask> all_tasks;

    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    for (const auto & remote_request : remote_requests)
    {
        pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
        remote_request.dag_request.SerializeToString(&(req->data));
        req->tp = pingcap::coprocessor::ReqType::DAG;
        req->start_ts = context.getSettingsRef().read_tso;
        req->schema_version = context.getSettingsRef().schema_version;
        req->resource_group_name = (*context.getDAGContext()).getResourceGroupName();

        pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
        pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
        std::multimap<std::string, std::string> meta_data;
        meta_data.emplace("is_remote_read", "true");

        auto tasks = pingcap::coprocessor::buildCopTaskForFullText(
            bo,
            cluster,
            remote_request.key_ranges,
            req,
            store_type,
            (*context.getDAGContext()).getKeyspaceID(),
            remote_request.connection_id,
            remote_request.connection_alias,
            &Poco::Logger::get("pingcap/coprocessor"),
            std::move(meta_data),
            [&] {},
            tici_scan.getTableId(),
            tici_scan.getIndexId(),
            tici_scan.getTiCIScan()->executor_id());
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }
    return all_tasks;
}

} // namespace DB

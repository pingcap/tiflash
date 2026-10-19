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

#include <Common/TiFlashMetrics.h>
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

namespace
{
coprocessor::ShardRetryReason toShardRetryReason(::ShardAdmissionCode code)
{
    switch (code)
    {
    case ::ShardAdmissionCode::Missing:
        return coprocessor::SHARD_RETRY_MISSING;
    case ::ShardAdmissionCode::EpochMismatch:
        return coprocessor::SHARD_RETRY_EPOCH_MISMATCH;
    case ::ShardAdmissionCode::ReaderBehind:
        return coprocessor::SHARD_RETRY_READER_BEHIND;
    case ::ShardAdmissionCode::ControlPlaneUnavailable:
        return coprocessor::SHARD_RETRY_CONTROL_PLANE_UNAVAILABLE;
    case ::ShardAdmissionCode::Ready:
        // StorageTantivy maps an inconsistent READY to ReaderBehind.
        return coprocessor::SHARD_RETRY_READER_BEHIND;
    }
    return coprocessor::SHARD_RETRY_REASON_UNSPECIFIED;
}
} // namespace

void StorageTantivyIterpreter::execute(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder)
{
    storage->splitRemoteReadAndLocalRead();
    const auto & executor_id = tici_scan.getTiCIScan()->executor_id();
    auto remote_shard_infos = storage->getRemoteShardInfos();
    const auto & admission_failures = storage->getAdmissionFailures();
    bool epoch_mismatch = false;
    for (const auto & failure : admission_failures)
    {
        const auto & result = failure.admission;
        epoch_mismatch |= result.code == ::ShardAdmissionCode::EpochMismatch;
        LOG_INFO(
            log,
            "tici shard admission rejected: executor_id={} shard_id={} code={} expected_epoch={} observed_epoch={} "
            "required_resolved_ts={} reader_applied_resolved_ts={} meta_persisted_resolved_ts={} retry_after_ms={}",
            executor_id,
            result.shard_id,
            static_cast<unsigned int>(result.code),
            result.expected_epoch,
            result.observed_epoch,
            result.required_resolved_ts,
            result.reader_applied_resolved_ts,
            result.meta_persisted_resolved_ts,
            result.retry_after_ms);
    }
    if (context.getDAGContext()->isBatchCop() && (!admission_failures.empty() || !remote_shard_infos.empty()))
    {
        std::vector<coprocessor::ShardRetryInfo> feedback;
        feedback.reserve(admission_failures.size() + remote_shard_infos.size());
        for (const auto & failure : admission_failures)
        {
            const auto & result = failure.admission;
            coprocessor::ShardRetryInfo info;
            info.set_shard_id(result.shard_id);
            info.set_reason(toShardRetryReason(result.code));
            info.set_retry_after_ms(result.retry_after_ms);
            info.set_observed_epoch(result.observed_epoch);
            info.set_reader_applied_resolved_ts(result.reader_applied_resolved_ts);
            info.set_meta_persisted_resolved_ts(result.meta_persisted_resolved_ts);
            feedback.push_back(std::move(info));
        }
        for (const auto & shard : remote_shard_infos)
        {
            coprocessor::ShardRetryInfo info;
            info.set_shard_id(shard.shard_id);
            info.set_reason(coprocessor::SHARD_RETRY_MISSING);
            feedback.push_back(std::move(info));
        }
        throw TiCIShardRetryException(std::move(feedback));
    }
    if (!admission_failures.empty())
    {
        throw RegionException(
            {},
            epoch_mismatch ? RegionException::RegionReadStatus::EPOCH_NOT_MATCH
                           : RegionException::RegionReadStatus::NOT_FOUND,
            "tici shard admission rejected");
    }
    if (!remote_shard_infos.empty())
    {
        if (context.getDAGContext()->isCop())
            throw RegionException({}, RegionException::RegionReadStatus::NOT_FOUND, "shard not found");
        context.getDAGContext()->retry_shards.insert(
            context.getDAGContext()->retry_shards.end(),
            remote_shard_infos.begin(),
            remote_shard_infos.end());
    }

    // local_read
    storage->read(exec_context, group_builder, Names(), SelectQueryInfo(), context, 0, max_streams);

    // remote_read
    const auto total_shards = tici_scan.getShardInfos().shard_info_list.size();
    const auto remote_shards = remote_shard_infos.size();
    const auto blocked_shards = admission_failures.size();
    const auto local_shards = total_shards - remote_shards - blocked_shards;
    LOG_INFO(
        log,
        "tici split shards: executor_id={} total={} local={} remote={} retry={} local_concurrency={} start_ts={}",
        executor_id,
        total_shards,
        local_shards,
        remote_shards,
        blocked_shards,
        group_builder.concurrency(),
        context.getSettingsRef().read_tso);

    auto remote_request = buildRemoteRequests(remote_shard_infos);
    if (!remote_request.empty())
    {
        PipelineExecGroupBuilder remote_builder;
        buildRemoteExec(exec_context, remote_builder, remote_request);
        if (!remote_builder.empty())
            LOG_INFO(
                log,
                "tici remote sourceOps built: executor_id={} remote_requests={} concurrency={} start_ts={}",
                executor_id,
                remote_request.size(),
                remote_builder.concurrency(),
                context.getSettingsRef().read_tso);
        group_builder.merge(std::move(remote_builder));
    }
}

std::vector<RemoteRequest> StorageTantivyIterpreter::buildRemoteRequests(ShardInfoList & remote_shard_infos)
{
    std::vector<RemoteRequest> remote_requests;
    if (remote_shard_infos.empty())
    {
        return remote_requests;
    }
    remote_requests.push_back(
        RemoteRequest::build(
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
            [&] { GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_sent).Increment(); },
            tici_scan.getTableId(),
            tici_scan.getIndexId(),
            tici_scan.getTiCIScan()->executor_id());
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_constructed)
        .Increment(static_cast<double>(all_tasks.size()));
    return all_tasks;
}

} // namespace DB

// Copyright 2025 PingCAP, Ltd.
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

#include <Common/TiFlashException.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/TantivyReaderSourceOp.h>
#include <Storages/IStorage.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageTantivy.h>
#include <Storages/Tantivy/TantivyInputStream.h>
#include <Storages/Tantivy/TiCIAdmission.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <tici-search-lib/src/lib.rs.h>

#include <cstddef>
#include <cstdlib>
#include <memory>

namespace DB
{

StorageTantivy::StorageTantivy(Context & context_, const TiCIScan & tici_scan_)
    : tici_scan(tici_scan_)
    , context(context_)
    , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
{}

BlockInputStreams StorageTantivy::read(
    const Names &,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned int)
{
    return {};
}

void StorageTantivy::read(
    PipelineExecutorContext & exec_status,
    PipelineExecGroupBuilder & group_builder,
    [[maybe_unused]] const Names & column_names,
    [[maybe_unused]] const SelectQueryInfo & info,
    const Context & context,
    [[maybe_unused]] size_t max_block_size,
    unsigned num_streams)
{
    NamesAndTypes return_columns;
    if (tici_scan.isCount())
    {
        RUNTIME_CHECK_MSG(tici_scan.getNamesAndTypes().size() == 1, "count search should return one column");
        return_columns = tici_scan.getNamesAndTypes();
    }
    else
    {
        return_columns = genNamesAndTypesForTiCI(tici_scan.getReturnColumns(), "column");
    }

    RUNTIME_CHECK(local_shards_snapshot.has_value());
    auto shards_snapshot = std::move(*local_shards_snapshot);
    local_shards_snapshot.reset();

    auto tici_task_pool = std::make_shared<TS::TiCIReadTaskPool>(
        log,
        tici_scan.getKeyspaceID(),
        tici_scan.getTableId(),
        tici_scan.getIndexId(),
        local_read,
        return_columns,
        tici_scan.getLimit(),
        tici_scan.getSortColumnIds(),
        tici_scan.getSortColumnAsc(),
        context.getSettingsRef().read_tso,
        tici_scan.getMatchExpr(),
        tici_scan.isCount(),
        context.getTimezoneInfo(),
        std::move(shards_snapshot));

    num_streams = std::max(1, std::min(num_streams, local_read.size()));
    // local read
    for (size_t i = 0; i < num_streams; ++i)
    {
        group_builder.addConcurrency(
            std::make_unique<TantivyReaderSourceOp>(exec_status, log->identifier(), tici_task_pool, return_columns));
    }
    if (!tici_scan.isCount())
        executeCastAfterTiCIScan(exec_status, group_builder);
}

void StorageTantivy::executeCastAfterTiCIScan(
    PipelineExecutorContext & exec_status,
    PipelineExecGroupBuilder & group_builder)
{
    if (group_builder.concurrency() == 0)
        return;
    // execute timezone cast or duration cast if needed for local tici scan
    DAGExpressionAnalyzer analyzer{group_builder.getCurrentHeader(), context};
    ExpressionActionsChain chain;
    std::vector<UInt8> may_need_add_cast_column;
    for (size_t i = 0; i < tici_scan.getReturnColumns().size(); ++i)
        may_need_add_cast_column.push_back(true);
    if (analyzer.appendExtraCastsAfterTiCI(chain, may_need_add_cast_column, tici_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        assert(extra_cast);
        chain.finalize();
        chain.clear();
        for (size_t i = 0; i < group_builder.concurrency(); ++i)
        {
            auto & builder = group_builder.getCurBuilder(i);
            builder.appendTransformOp(
                std::make_unique<ExpressionTransformOp>(exec_status, log->identifier(), extra_cast));
        }
    }
}

void StorageTantivy::splitRemoteReadAndLocalRead()
{
    const auto & all = tici_scan.getShardInfos().shard_info_list;
    ShardInfoList local_shard_infos;
    ShardInfoList remote_shard_infos;
    std::vector<TiCIAdmissionFailure> blocked;
    ::rust::Vec<::ShardFreshnessRequirement> requirements;
    for (const auto & shard_info : all)
    {
        RUNTIME_CHECK_MSG(
            shard_info.freshness_mode <= static_cast<UInt8>(::ShardFreshnessMode::BoundedStale),
            "unknown TiCI freshness mode {} for shard {}",
            shard_info.freshness_mode,
            shard_info.shard_id);
        requirements.push_back(
            ::ShardFreshnessRequirement{
                .shard_id = shard_info.shard_id,
                .expected_epoch = shard_info.shard_epoch,
                .required_resolved_ts = shard_info.required_resolved_ts,
                .mode = static_cast<::ShardFreshnessMode>(shard_info.freshness_mode),
            });
    }

    ::rust::Vec<::ShardAdmissionResult> results;
    local_shards_snapshot = acquire_shards_snapshot_v2(requirements, results);
    RUNTIME_CHECK(local_shards_snapshot.has_value());
    RUNTIME_CHECK(results.size() == all.size());
    for (size_t i = 0; i < all.size(); ++i)
    {
        auto admission = results[i];
        RUNTIME_CHECK_MSG(
            admission.shard_id == all[i].shard_id,
            "TiCI admission result shard {} does not match requested shard {}",
            admission.shard_id,
            all[i].shard_id);
        RUNTIME_CHECK_MSG(
            admission.expected_epoch == requirements[i].expected_epoch
                && admission.required_resolved_ts == requirements[i].required_resolved_ts,
            "TiCI admission result for shard {} does not match its requirement",
            admission.shard_id);
        const auto route = routeTiCIShard(requirements[i], admission);
        if (route == TiCIAdmissionRoute::Local)
        {
            LOG_DEBUG(
                log,
                "tici shard admitted: shard_id={} epoch={} manifest_seq={} required_resolved_ts={} "
                "reader_applied_resolved_ts={} meta_persisted_resolved_ts={}",
                admission.shard_id,
                admission.observed_epoch,
                admission.observed_manifest_seq,
                admission.required_resolved_ts,
                admission.reader_applied_resolved_ts,
                admission.meta_persisted_resolved_ts);
            local_shard_infos.push_back(all[i]);
        }
        else if (route == TiCIAdmissionRoute::Remote)
        {
            remote_shard_infos.push_back(all[i]);
        }
        else
        {
            // Defend the request contract even if the FFI reports READY with
            // an insufficient watermark.
            if (admission.code == ::ShardAdmissionCode::Ready)
                admission.code = ::ShardAdmissionCode::ReaderBehind;
            blocked.push_back(TiCIAdmissionFailure{all[i], admission});
        }
    }
    remote_read = std::move(remote_shard_infos);
    local_read = std::move(local_shard_infos);
    admission_failures = std::move(blocked);
}

} // namespace DB

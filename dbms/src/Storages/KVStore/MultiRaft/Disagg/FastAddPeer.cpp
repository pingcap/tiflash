// Copyright 2023 PingCAP, Inc.
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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/KVStore/MultiRaft/Disagg/ServerlessUtils.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/StorageDeltaMerge.h>
#include <fmt/core.h>

#include <memory>

namespace DB
{

namespace FailPoints
{
extern const char force_fap_worker_throw[];
extern const char force_set_fap_candidate_store_id[];
} // namespace FailPoints

/// FastAddPeer Result Metrics:
/// - type_total counts the total number of FAP tasks.
/// - type_success_transform counts how many FAP snapshots have been written.
/// - type_failed_other counts errors not originated from FAP, such as a thread pool failure.
/// - type_failed_cancel counts when a FAP task is canceled outside of the FAP builder thread, due to either timeout or not.
/// - type_failed_no_suitable / type_failed_no_candidate are failures in selecting stage.
/// - type_failed_repeated is a special case where we found an FAP snapshot exists when we are doing write staging.
/// - type_failed_baddata counts when a exception throws from FAP builder thread, mostly it could be a unhandled exception from UniPS or DeltaTree.
///   It results in a `BadData` returncode to Proxy.
/// - type_failed_build_chkpt is used especially to track when exception throws when building segments in write stage.
/// - type_reuse_chkpt_cache counts how many times a checkpoint is reused to build an FAP snapshot.
/// - type_restore counts the number FAP snapshot is restored for applying after a restart.
/// - type_succeed count succeed FAP tasks, when an FAP snapshot is applied.

using raft_serverpb::PeerState;
using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

FastAddPeerRes genFastAddPeerRes(
    FastAddPeerStatus status,
    std::string && apply_str,
    std::string && region_str,
    uint64_t shard_ver,
    std::string && inner_key_str,
    std::string && enc_key_str,
    std::string && txn_file_ref_str)
{
    auto * apply = RawCppString::New(apply_str);
    auto * region = RawCppString::New(region_str);
    auto * inner_key = RawCppString::New(inner_key_str);
    auto * enc_key = RawCppString::New(enc_key_str);
    auto * txn_file_ref = RawCppString::New(txn_file_ref_str);
    return FastAddPeerRes{
        .status = status,
        .apply_state = CppStrWithView{.inner = GenRawCppPtr(apply, RawCppPtrTypeImpl::String), .view = BaseBuffView{apply->data(), apply->size()}},
        .region = CppStrWithView{.inner = GenRawCppPtr(region, RawCppPtrTypeImpl::String), .view = BaseBuffView{region->data(), region->size()}},
        .shard_ver = shard_ver,
        .inner_key = CppStrWithView{.inner = GenRawCppPtr(inner_key, RawCppPtrTypeImpl::String), .view = BaseBuffView{inner_key->data(), inner_key->size()}},
        .enc_key = CppStrWithView{.inner = GenRawCppPtr(enc_key, RawCppPtrTypeImpl::String), .view = BaseBuffView{enc_key->data(), enc_key->size()}},
        .txn_file_ref = CppStrWithView{.inner = GenRawCppPtr(txn_file_ref, RawCppPtrTypeImpl::String), .view = BaseBuffView{txn_file_ref->data(), txn_file_ref->size()}},
    };
}

FastAddPeerRes genFastAddPeerResFail(FastAddPeerStatus status)
{
    return genFastAddPeerRes(status, "", "", 0, "", "", "");
}

std::vector<StoreID> getCandidateStoreIDsForRegion(TMTContext & tmt_context, UInt64 region_id, UInt64 current_store_id)
{
    fiu_do_on(FailPoints::force_set_fap_candidate_store_id, { return {1234}; });
    auto pd_client = tmt_context.getPDClient();
    auto resp = pd_client->getRegionByID(region_id);
    const auto & region = resp.region();
    std::vector<StoreID> store_ids;
    store_ids.reserve(region.peers_size());
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == current_store_id)
            continue;
        // TODO: use label on the store to determine whether it's tiflash WN
        if (peer.role() == metapb::PeerRole::Learner)
        {
            store_ids.push_back(peer.store_id());
        }
    }
    return store_ids;
}

std::optional<CheckpointRegionInfoAndData> tryParseRegionInfoFromCheckpointData(
    ParsedCheckpointDataHolderPtr checkpoint_data_holder,
    UInt64 remote_store_id,
    UInt64 region_id,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");
    RegionPtr region;
    {
        auto region_key = UniversalPageIdFormat::toKVStoreKey(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()
                        ->read(region_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            region = Region::deserialize(buf, proxy_helper);
        }
        else
        {
            GET_METRIC(tiflash_fap_nomatch_reason, type_no_meta).Increment();
            return std::nullopt;
        }
    }

    RaftApplyState apply_state;
    {
        // TODO: use `RaftDataReader::readRegionApplyState`?
        auto apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()
                        ->read(apply_state_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            apply_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            GET_METRIC(tiflash_fap_nomatch_reason, type_no_meta).Increment();
            LOG_DEBUG(log, "Failed to find apply state key, region_id={}", region_id);
            return std::nullopt;
        }
    }

    RegionLocalState region_state;
    {
        auto local_state_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()
                        ->read(local_state_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            region_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            GET_METRIC(tiflash_fap_nomatch_reason, type_no_meta).Increment();
            LOG_DEBUG(log, "Failed to find region local state key region_id={}", region_id);
            return std::nullopt;
        }
    }

    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    checkpoint_info->remote_store_id = remote_store_id;
    checkpoint_info->region_id = region_id;
    checkpoint_info->checkpoint_data_holder = checkpoint_data_holder;
    checkpoint_info->temp_ps = checkpoint_data_holder->getUniversalPageStorage();
    return std::make_tuple(checkpoint_info, region, apply_state, region_state);
}

bool tryResetPeerIdInRegion(RegionPtr region, const RegionLocalState & region_state, uint64_t new_peer_id)
{
    auto peer_state = region_state.state();
    if (peer_state == PeerState::Tombstone || peer_state == PeerState::Applying)
    {
        GET_METRIC(tiflash_fap_nomatch_reason, type_region_state).Increment();
        return false;
    }
    for (const auto & peer : region_state.region().peers())
    {
        if (peer.id() == new_peer_id)
        {
            auto peer_copy = peer;
            region->mutMeta().setPeer(std::move(peer_copy));
            return true;
        }
    }
    GET_METRIC(tiflash_fap_nomatch_reason, type_conf).Increment();
    return false;
}

std::variant<CheckpointRegionInfoAndData, FastAddPeerRes> FastAddPeerImplSelect(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    uint64_t region_id,
    uint64_t new_peer_id)
{
    GET_METRIC(tiflash_fap_task_state, type_selecting_stage).Increment();
    SCOPE_EXIT({ GET_METRIC(tiflash_fap_task_state, type_selecting_stage).Decrement(); });

    auto log = Logger::get("FastAddPeer");
    Stopwatch watch;
    std::unordered_map<StoreID, UInt64> checked_seq_map;
    auto fap_ctx = tmt.getContext().getSharedContextDisagg()->fap_context;
    RUNTIME_CHECK(fap_ctx != nullptr);
    RUNTIME_CHECK(fap_ctx->tasks_trace != nullptr);
    auto cancel_handle = fap_ctx->tasks_trace->getCancelHandleFromExecutor(region_id);

    // Get candidate stores.
    const auto & settings = tmt.getContext().getSettingsRef();
    const auto current_store_id = tmt.getKVStore()->clonedStoreMeta().id();
    std::vector<StoreID> candidate_store_ids = getCandidateStoreIDsForRegion(tmt, region_id, current_store_id);

    fiu_do_on(FailPoints::force_fap_worker_throw, { throw Exception(ErrorCodes::LOGICAL_ERROR, "mocked throw"); });

    // It could be the first TiFlash peer(most cases), thus there's no candidate for FAP at all.
    // NOTE that it is unpredictable that which TiFlash node are scheduled for the peer first,
    // it could be always TiFlash node "p1" if we schedule from 0 replica -> 2 replica.
    if (candidate_store_ids.empty())
    {
        LOG_DEBUG(log, "No suitable candidate peer for region_id={}", region_id);
        GET_METRIC(tiflash_fap_task_result, type_failed_no_candidate).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::NoSuitable);
    }
    LOG_DEBUG(log, "Begin to select checkpoint for region_id={}", region_id);

    // It will return with FastAddPeerRes or failed with timeout result wrapped in FastAddPeerRes.
    while (true)
    {
        // Check all candidate stores in this loop.
        for (const auto store_id : candidate_store_ids)
        {
            RUNTIME_CHECK(store_id != current_store_id, store_id, current_store_id);
            auto iter = checked_seq_map.find(store_id);
            auto checked_seq = (iter == checked_seq_map.end()) ? 0 : iter->second;
            auto [data_seq, checkpoint_data] = fap_ctx->getNewerCheckpointData(tmt.getContext(), store_id, checked_seq);

            checked_seq_map[store_id] = data_seq;
            if (data_seq <= checked_seq)
                continue;
            RUNTIME_CHECK(checkpoint_data != nullptr);
            auto maybe_region_info
                = tryParseRegionInfoFromCheckpointData(checkpoint_data, store_id, region_id, proxy_helper);
            if (!maybe_region_info.has_value())
                continue;
            const auto & checkpoint_info = std::get<0>(maybe_region_info.value());
            auto & region = std::get<1>(maybe_region_info.value());
            auto & region_state = std::get<3>(maybe_region_info.value());
            if (tryResetPeerIdInRegion(region, region_state, new_peer_id))
            {
                LOG_INFO(
                    log,
                    "Select checkpoint with data_seq={}, remote_store_id={} elapsed={} size(candidate_store_id)={} "
                    "region_id={} region={} range={}",
                    data_seq,
                    checkpoint_info->remote_store_id,
                    watch.elapsedSeconds(),
                    candidate_store_ids.size(),
                    region_id,
                    region->getDebugString(),
                    region->getRange()->toDebugString());
                GET_METRIC(tiflash_fap_task_duration_seconds, type_select_stage).Observe(watch.elapsedSeconds());
                return maybe_region_info.value();
            }
            else
            {
                LOG_DEBUG(
                    log,
                    "Checkpoint with seq {} doesn't contain reusable region info region_id={} from_store_id={}",
                    data_seq,
                    region_id,
                    store_id);
            }
        }
        {
            if (watch.elapsedSeconds() >= settings.fap_wait_checkpoint_timeout_seconds)
            {
                // This could happen if the checkpoint we got is not fresh enough.
                LOG_INFO(
                    log,
                    "FastAddPeer timeout when select checkpoints region_id={} new_peer_id={}",
                    region_id,
                    new_peer_id);
                GET_METRIC(tiflash_fap_task_result, type_failed_no_suitable).Increment();
                return genFastAddPeerResFail(FastAddPeerStatus::NoSuitable);
            }
            SYNC_FOR("in_FastAddPeerImplSelect::before_sleep");
            if (cancel_handle->blockedWaitFor(std::chrono::milliseconds(1000)))
            {
                LOG_INFO(log, "FAP is canceled during peer selecting, region_id={}", region_id);
                // Just remove the task from AsyncTasks, it will not write anything in disk during this stage.
                // NOTE once canceled, Proxy should no longer polling `FastAddPeer`, since it will result in `OtherError`.
                fap_ctx->tasks_trace->leakingDiscardTask(region_id);
                // We immediately increase this metrics when cancel, since a canceled task may not be fetched.
                GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
                return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
            }
        }
    }
}

FastAddPeerRes FastAddPeerImplWrite(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 new_peer_id,
    CheckpointRegionInfoAndData && checkpoint,
    UInt64 start_time)
{
    auto fap_ctx = tmt.getContext().getSharedContextDisagg()->fap_context;
    auto cancel_handle = fap_ctx->tasks_trace->getCancelHandleFromExecutor(region_id);
    const auto & settings = tmt.getContext().getSettingsRef();
    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage).Observe(watch.elapsedSeconds()); });
    GET_METRIC(tiflash_fap_task_state, type_writing_stage).Increment();
    SCOPE_EXIT({ GET_METRIC(tiflash_fap_task_state, type_writing_stage).Decrement(); });

    auto [checkpoint_info, region, apply_state, region_state] = checkpoint;

    auto keyspace_id = region->getKeyspaceID();
    auto table_id = region->getMappedTableID();

    auto log = Logger::get(fmt::format(
        "FastAddPeer(region_id={} keyspace={} table_id={} new_peer_id={})",
        region_id,
        keyspace_id,
        table_id,
        new_peer_id));

    const auto [table_drop_lock, storage, schema_snap] = AtomicGetStorageSchema(region_id, keyspace_id, table_id, tmt);
    if (!storage)
    {
        LOG_WARNING(log, "FAP failed because the table can not be found");
        return genFastAddPeerResFail(FastAddPeerStatus::BadData);
    }
    UNUSED(schema_snap);
    RUNTIME_CHECK_MSG(storage->engineType() == TiDB::StorageEngine::DT, "ingest into unsupported storage engine");
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto new_key_range = DM::RowKeyRange::fromRegionRange(
        region->getRange(),
        table_id,
        storage->isCommonHandle(),
        storage->getRowKeyColumnSize(),
        fmt::format("region_id={} new_peer_id={} FastAddPeerImplWrite", region_id, new_peer_id));

    if (cancel_handle->isCanceled())
    {
        LOG_INFO(log, "FAP is canceled before write");
        fap_ctx->cleanTask(tmt, proxy_helper, region_id, CheckpointIngestInfo::CleanReason::TiFlashCancel);
        GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
    }

    DM::Segments segments;
    try
    {
        LOG_INFO(log, "FAP begins to build segments, range={}", new_key_range.toDebugString());
        segments = dm_storage->buildSegmentsFromCheckpointInfo(cancel_handle, new_key_range, checkpoint_info, settings);
    }
    catch (...)
    {
        // It will call `createTargetSegmentsFromCheckpoint`, which will build delta and stable space for all segments.
        // For every remote pages refered, `createS3LockForWriteBatch` will lock them on S3 to prevent them from being GC-ed.
        // Failure in creating lock results in an Exception, causing FAP fallback with BadData error.
        // A typical failure is that this TiFlash node fails to communicate with other TiFlash nodes.
        GET_METRIC(tiflash_fap_task_result, type_failed_build_chkpt).Increment();
        throw;
    }
    GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage_build).Observe(watch.elapsedSecondsFromLastTime());

    // Note that the task may be canceled during `buildSegmentsFromCheckpointInfo`. So we must check here before further operations.
    if (cancel_handle->isCanceled())
    {
        LOG_INFO(log, "FAP is canceled after build segments");
        fap_ctx->cleanTask(tmt, proxy_helper, region_id, CheckpointIngestInfo::CleanReason::TiFlashCancel);
        GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
    }

    UniversalWriteBatch wb;
    RUNTIME_CHECK(checkpoint_info->temp_ps != nullptr);
    RaftDataReader raft_data_reader(*(checkpoint_info->temp_ps));

    bool too_many_raft_logs = false;
    size_t log_count = 0;
    raft_data_reader.traverseRemoteRaftLogForRegion(
        region_id,
        [&](size_t raft_log_count) {
            GET_METRIC(tiflash_raft_raft_log_gap_count, type_unhandled_fap_raft_log).Observe(raft_log_count);
            log_count = raft_log_count;
            if (raft_log_count > 1500)
            {
                too_many_raft_logs = true;
                // Will early abort.
                return false;
            }
            return true;
        },
        [&](const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation & location) {
            wb.putRemotePage(page_id, 0, size, location, {});
        });

    if (too_many_raft_logs)
    {
        LOG_INFO(
            log,
            "FAP is canceled when write raft log, too_many_raft_logs={}, log_count={}, applied_index={}",
            too_many_raft_logs,
            log_count,
            apply_state.applied_index());
        fap_ctx->cleanTask(tmt, proxy_helper, region_id, CheckpointIngestInfo::CleanReason::TiFlashCancel);
        GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
    }

    SYNC_FOR("in_FastAddPeerImplWrite::after_build_raft_log");
    if (cancel_handle->isCanceled())
    {
        LOG_INFO(log, "FAP is canceled after build raft log");
        fap_ctx->cleanTask(tmt, proxy_helper, region_id, CheckpointIngestInfo::CleanReason::TiFlashCancel);
        GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
    }

    // Write the fap snapshot.
    fap_ctx->insertCheckpointIngestInfo(
        tmt,
        region_id,
        new_peer_id,
        checkpoint_info->remote_store_id,
        region,
        std::move(segments),
        start_time);
    GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage_insert).Observe(watch.elapsedSecondsFromLastTime());

    SYNC_FOR("in_FastAddPeerImplWrite::after_write_segments");
    if (cancel_handle->isCanceled())
    {
        LOG_INFO(log, "FAP is canceled after write segments");
        fap_ctx->cleanTask(tmt, proxy_helper, region_id, CheckpointIngestInfo::CleanReason::TiFlashCancel);
        GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
        return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
    }

    // Now, the FAP snapshot is persisted. And we will later send it to ourselves to have it acked by the raftstore.
    // Later, we directly write the raft log into formal unips.
    // Since a raft snapshot doestn't hold raft logs, whether these logs is written doesn't affect the applying stage.
    // That's the reason why we write those logs after FAP snapshot is written.
    // However, we still write them in order to provide some later logs, so that the new peer is less likely to lag.
    // TODO(fap) Currently, FAP only handle when the peer is newly created in this store,
    // Move this to `ApplyFapSnapshot` and clean stale data, if FAP can later handle all snapshots.
    GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage_raft).Observe(watch.elapsedSecondsFromLastTime());
    auto wn_ps = tmt.getContext().getWriteNodePageStorage();
    RUNTIME_CHECK(wn_ps != nullptr);
    wn_ps->write(std::move(wb));
    LOG_DEBUG(log, "Finish write FAP snapshot, log_count={}", log_count);
    auto tmp_ps = checkpoint_info->checkpoint_data_holder->getUniversalPageStorage();
    return genFastAddPeerRes(
        FastAddPeerStatus::Ok,
        apply_state.SerializeAsString(),
        region_state.region().SerializeAsString(),
        getShardVer(tmp_ps, region_id),
        getCompactibleInnerKey(tmp_ps, keyspace_id, region_id),
        getCompactibleEncKey(tmp_ps, keyspace_id, region_id),
        getTxnFileRef(tmp_ps, region_id));
}

// This function executes FAP phase 1 from a thread in a dedicated pool.
FastAddPeerRes FastAddPeerImpl(
    FastAddPeerContextPtr fap_ctx,
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 new_peer_id,
    UInt64 start_time)
{
    auto log = Logger::get("FastAddPeer");
    try
    {
        auto maybe_elapsed = fap_ctx->tasks_trace->queryElapsed(region_id);
        if unlikely (!maybe_elapsed.has_value())
        {
            GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
            LOG_INFO(log, "FAP is canceled at beginning region_id={} new_peer_id={}", region_id, new_peer_id);
        }
        auto elapsed = maybe_elapsed.value();
        GET_METRIC(tiflash_fap_task_duration_seconds, type_queue_stage).Observe(elapsed / 1000.0);
        GET_METRIC(tiflash_fap_task_state, type_queueing_stage).Decrement();
        // We don't delete fap snapshot if exists. However, there could be the following case:
        // - Phase 1 generates an fap snapshot and TiFlash restarts before it could send faked snapshot.
        // - Another phase 1 is started because the peer is not inited.
        // - The phase 1 fallbacked. Leaving the FAP snapshot of previous phase 1.
        // It is OK to preserve the stale fap snapshot, because we will compare (index, term) before pre/post apply.
        auto res = FastAddPeerImplSelect(tmt, proxy_helper, region_id, new_peer_id);
        if (std::holds_alternative<CheckpointRegionInfoAndData>(res))
        {
            auto final_res = FastAddPeerImplWrite(
                tmt,
                proxy_helper,
                region_id,
                new_peer_id,
                std::move(std::get<CheckpointRegionInfoAndData>(res)),
                start_time);
            if (final_res.status == FastAddPeerStatus::Ok)
                GET_METRIC(tiflash_fap_task_result, type_success_transform).Increment();
            return final_res;
        }
        return std::get<FastAddPeerRes>(res);
    }
    catch (const Exception & e)
    {
        // Could be like:
        // - can't put remote page with empty data_location
        DB::tryLogCurrentException(
            "FastAddPeerImpl",
            fmt::format(
                "Failed when try to restore from checkpoint region_id={} new_peer_id={} {}",
                region_id,
                new_peer_id,
                e.message()));
        GET_METRIC(tiflash_fap_task_result, type_failed_baddata).Increment();
        // The task could stuck in AsyncTasks as Finished till fetched by resolveFapSnapshotState,
        // since a FastAddPeerStatus::BadData result will lead to a fallback in Proxy.
        return genFastAddPeerResFail(FastAddPeerStatus::BadData);
    }
    catch (...)
    {
        DB::tryLogCurrentException(
            "FastAddPeerImpl",
            fmt::format(
                "Failed when try to restore from checkpoint region_id={} new_peer_id={}",
                region_id,
                new_peer_id));
        GET_METRIC(tiflash_fap_task_result, type_failed_baddata).Increment();
        // The task could stuck in AsyncTasks as Finished till fetched by resolveFapSnapshotState.
        // since a FastAddPeerStatus::BadData result will lead to a fallback in Proxy.
        return genFastAddPeerResFail(FastAddPeerStatus::BadData);
    }
}

uint8_t ApplyFapSnapshotImpl(
    TMTContext & tmt,
    TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id,
    bool assert_exist,
    UInt64 index,
    UInt64 term)
{
    auto log = Logger::get("FastAddPeer");
    Stopwatch watch_ingest;
    auto kvstore = tmt.getKVStore();
    auto fap_ctx = tmt.getContext().getSharedContextDisagg()->fap_context;
    auto checkpoint_ingest_info = fap_ctx->getOrRestoreCheckpointIngestInfo(tmt, proxy_helper, region_id, peer_id);
    if (!checkpoint_ingest_info)
    {
        if (assert_exist)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected to have fap snapshot, region_id={}, peer_id={}",
                region_id,
                peer_id);
        }
        // If fap is enabled, and this region is not currently exists on proxy's side,
        // proxy will check if we have a fap snapshot first.
        // If we don't, the snapshot should be a regular snapshot.
        LOG_DEBUG(
            log,
            "Failed to get fap snapshot, it's regular snapshot, region_id={}, peer_id={}",
            region_id,
            peer_id);
        return false;
    }
    auto begin = checkpoint_ingest_info->beginTime();
    bool is_repeated = false;
    if (auto kvr = kvstore->getRegion(region_id); kvr != nullptr)
    {
        if (kvr->appliedIndex() == index && kvr->appliedIndexTerm() == term)
        {
            LOG_INFO(
                log,
                "Duplicate FAP snapshot ingest, maybe replayed after restart, region_id={} peer_id={} begin_time={} "
                "index={} term={}",
                region_id,
                peer_id,
                begin,
                index,
                term);
            is_repeated = true;
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Don't support FAP for an existing region, region_id={} peer_id={} begin_time={} kvstore=({},{}) "
                "snapshot=({},{})",
                region_id,
                peer_id,
                begin,
                kvr->appliedIndex(),
                kvr->appliedIndexTerm(),
                index,
                term);
        }
    }
    // `region_to_ingest` is not the region in kvstore.
    auto region_to_ingest = checkpoint_ingest_info->getRegion();
    RUNTIME_CHECK(region_to_ingest != nullptr);
    if (region_to_ingest->appliedIndex() != index || region_to_ingest->appliedIndexTerm() != term)
    {
        if (assert_exist)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatched region and term, expected=({},{}) actual=({},{}) region_id={} peer_id={} begin_time={} "
                "is_repeated={}",
                index,
                term,
                region_to_ingest->appliedIndex(),
                region_to_ingest->appliedIndexTerm(),
                region_id,
                peer_id,
                begin,
                is_repeated);
        }
        else
        {
            LOG_DEBUG(log, "Fap snapshot not match, maybe stale, region_id={}, peer_id={}", region_id, peer_id);
            return false;
        }
    }
    LOG_INFO(
        log,
        "Begin apply fap snapshot, region_id={} peer_id={} begin_time={} is_repeated={}",
        region_id,
        peer_id,
        begin,
        is_repeated);
    // If there is `checkpoint_ingest_info`, it is exactly the data we want to ingest. Consider two scene:
    // 1. If there was a failed FAP which failed to clean, its data will be overwritten by current FAP which has finished phase 1.
    // 2. It is not possible that a restart happens at FAP phase 2, and a regular snapshot is sent, because snapshots can only be accepted once the previous snapshot it handled.
    {
        GET_METRIC(tiflash_fap_task_state, type_ingesting_stage).Increment();
        SCOPE_EXIT({ GET_METRIC(tiflash_fap_task_state, type_ingesting_stage).Decrement(); });
        kvstore->handleIngestCheckpoint(checkpoint_ingest_info->getRegion(), checkpoint_ingest_info, tmt);
        // Delayed the clean stage until the proxy has persisted the ingestion after apply the fap snapshot
        GET_METRIC(tiflash_fap_task_duration_seconds, type_ingest_stage).Observe(watch_ingest.elapsedSeconds());
        auto current = FAPAsyncTasks::getCurrentMillis();
        auto elapsed = (current - begin) / 1000.0;
        if (begin != 0)
        {
            GET_METRIC(tiflash_fap_task_duration_seconds, type_total).Observe(elapsed);
        }
        LOG_INFO(
            log,
            "Finish apply fap snapshot, region_id={} peer_id={} elapsed={} is_repeated={}",
            region_id,
            peer_id,
            elapsed,
            is_repeated);
        GET_METRIC(tiflash_fap_task_result, type_succeed).Increment();
        return true;
    }
}

FapSnapshotState QueryFapSnapshotState(
    EngineStoreServerWrap * server,
    uint64_t region_id,
    uint64_t peer_id,
    uint64_t index,
    uint64_t term)
{
    // It's called before pre_apply_snapshot, if we have an good fap snapshot, we use it.
    // It's also called before post_apply_snapshot, if we have an good fap snapshot, we use it.
    // If we don't, we will check if the arriving snapshot is a fap snapshot, and if so, we panic.
    try
    {
        RUNTIME_CHECK_MSG(server->tmt, "TMTContext is null");
        RUNTIME_CHECK_MSG(server->proxy_helper, "proxy_helper is null");
        if (!server->tmt->getContext().getSharedContextDisagg()->isDisaggregatedStorageMode())
            return FapSnapshotState::Other;
        auto fap_ctx = server->tmt->getContext().getSharedContextDisagg()->fap_context;
        // We just restore it, since if there is, it will soon be used.
        if (auto ptr
            = fap_ctx->getOrRestoreCheckpointIngestInfo(*(server->tmt), server->proxy_helper, region_id, peer_id);
            ptr != nullptr)
        {
            RUNTIME_CHECK(ptr->getRegion() != nullptr);
            if (ptr->getRegion()->appliedIndex() == index && ptr->getRegion()->appliedIndexTerm() == term)
                return FapSnapshotState::Persisted;
            return FapSnapshotState::NotFound;
        }
        return FapSnapshotState::NotFound;
    }
    catch (...)
    {
        // If any exception happened, we consider the FAP snapshot is not found.
        LOG_ERROR(
            DB::Logger::get("QueryFapSnapshotState"),
            "Failed query fap snapshot state region_id={} peer_id={}",
            region_id,
            peer_id);
        GET_METRIC(tiflash_fap_task_result, type_failed_query_state).Increment();
        return FapSnapshotState::NotFound;
    }
}

uint8_t ApplyFapSnapshot(
    EngineStoreServerWrap * server,
    uint64_t region_id,
    uint64_t peer_id,
    uint8_t assert_exist,
    uint64_t index,
    uint64_t term)
{
    try
    {
        RUNTIME_CHECK_MSG(server->tmt, "TMTContext is null");
        RUNTIME_CHECK_MSG(server->proxy_helper, "proxy_helper is null");
        if (!server->tmt->getContext().getSharedContextDisagg()->isDisaggregatedStorageMode())
            return false;
        return ApplyFapSnapshotImpl(*server->tmt, server->proxy_helper, region_id, peer_id, assert_exist, index, term);
    }
    catch (...)
    {
        DB::tryLogCurrentFatalException(
            "FastAddPeerApply",
            fmt::format("Failed when try to apply fap snapshot region_id={} peer_id={}", region_id, peer_id));
        exit(-1);
    }
}

FastAddPeerRes FastAddPeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto log = Logger::get("FastAddPeer");
        if (!server->tmt->getContext().getSharedContextDisagg()->isDisaggregatedStorageMode())
            return genFastAddPeerResFail(FastAddPeerStatus::OtherError);
        auto fap_ctx = server->tmt->getContext().getSharedContextDisagg()->fap_context;
        if (fap_ctx == nullptr)
        {
            LOG_WARNING(log, "FAP Context is not initialized. Should only enable FAP in DisaggregatedStorageMode.");
            return genFastAddPeerResFail(FastAddPeerStatus::NoSuitable);
        }
        if (!fap_ctx->tasks_trace->isScheduled(region_id))
        {
            // We need to schedule the task.
            auto current_time = FAPAsyncTasks::getCurrentMillis();
            GET_METRIC(tiflash_fap_task_state, type_queueing_stage).Increment();
            GET_METRIC(tiflash_fap_task_result, type_total).Increment();
            auto job_func = [server, region_id, new_peer_id, fap_ctx, current_time]() {
                std::string origin_name = getThreadName();
                SCOPE_EXIT({ setThreadName(origin_name.c_str()); });
                setThreadName("fap-builder");
                return FastAddPeerImpl(
                    fap_ctx,
                    *(server->tmt),
                    server->proxy_helper,
                    region_id,
                    new_peer_id,
                    current_time);
            };
            auto res = fap_ctx->tasks_trace->addTaskWithCancel(region_id, job_func, [log, region_id, new_peer_id]() {
                LOG_INFO(
                    log,
                    "FAP is canceled in queue due to timeout region_id={} new_peer_id={}",
                    region_id,
                    new_peer_id);
                // It is already canceled in queue.
                GET_METRIC(tiflash_fap_task_result, type_failed_cancel).Increment();
                return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
            });
            if (res)
            {
                GET_METRIC(tiflash_fap_task_state, type_ongoing).Increment();
                LOG_INFO(log, "Add new task success, new_peer_id={} region_id={}", new_peer_id, region_id);
            }
            else
            {
                // If the queue is full, the task won't be registered, return OtherError for quick fallback.
                // If proxy still mistakenly polls canceled task, it will also fails here.
                LOG_ERROR(
                    log,
                    "Add new task fail(queue full) or poll canceled, new_peer_id={} region_id={}",
                    new_peer_id,
                    region_id);
                GET_METRIC(tiflash_fap_task_result, type_failed_other).Increment();
                return genFastAddPeerResFail(FastAddPeerStatus::OtherError);
            }
        }

        // If the task is canceled, the task will not be `isScheduled`.
        if (fap_ctx->tasks_trace->isReady(region_id))
        {
            GET_METRIC(tiflash_fap_task_state, type_ongoing).Decrement();
            auto [result, elapsed] = fap_ctx->tasks_trace->fetchResultAndElapsed(region_id);
            LOG_INFO(
                log,
                "Fetch task, result={} new_peer_id={} region_id={} elapsed={}",
                magic_enum::enum_name(result.status),
                new_peer_id,
                region_id,
                elapsed);
            GET_METRIC(tiflash_fap_task_duration_seconds, type_phase1_total).Observe(elapsed / 1000.0);
            return result;
        }
        else
        {
            const auto & settings = server->tmt->getContext().getSettingsRef();
            auto maybe_elapsed = fap_ctx->tasks_trace->queryElapsed(region_id);
            RUNTIME_CHECK_MSG(
                maybe_elapsed.has_value(),
                "Task not found, region_id={} new_peer_id={}",
                region_id,
                new_peer_id);
            auto elapsed = maybe_elapsed.value();
            if (elapsed >= 1000 * settings.fap_task_timeout_seconds)
            {
                /// NOTE: Make sure FastAddPeer is the only place to cancel FAP phase 1.
                // If the task is running, we have to wait it return on cancel and clean,
                // otherwise a later regular may race with this clean.
                auto prev_state = fap_ctx->tasks_trace->queryState(region_id);
                LOG_INFO(
                    log,
                    "Cancel FAP due to timeout region_id={} new_peer_id={} prev_state={} elapsedMillis={} threshold={}",
                    region_id,
                    new_peer_id,
                    magic_enum::enum_name(prev_state),
                    elapsed,
                    settings.fap_task_timeout_seconds);
                GET_METRIC(tiflash_fap_task_state, type_blocking_cancel_stage).Increment();
                {
                    // TODO We can use `asyncCancelTask` here, and return WaitForData if the task is cancelling.
                    // Otherwise, we will let the next polling do the check again.
                    [[maybe_unused]] auto s = fap_ctx->tasks_trace->blockedCancelRunningTask(region_id);
                }
                // Previously, `fap_fallback_to_slow` could do the cleaning job,
                // however, it will not clean what's in memory.
                // So, if a msgSnapshot is sent, then a regular raft snapshot handling may be started,
                // and if the we failed to clean the fap snapshot by `fap_fallback_to_slow` before prehandling,
                // the process could panic.
                fap_ctx->cleanTask(
                    *(server->tmt),
                    server->proxy_helper,
                    region_id,
                    CheckpointIngestInfo::CleanReason::ProxyFallback);
                LOG_INFO(log, "Finished clean task from proxy region_id={} new_peer_id={}", region_id, new_peer_id);
                GET_METRIC(tiflash_fap_task_state, type_blocking_cancel_stage).Decrement();
                // Return Canceled because it is cancel from outside FAP worker.
                return genFastAddPeerResFail(FastAddPeerStatus::Canceled);
            }
            LOG_DEBUG(log, "Task is still pending new_peer_id={} region_id={}", new_peer_id, region_id);
            return genFastAddPeerResFail(FastAddPeerStatus::WaitForData);
        }
    }
    catch (const Exception & e)
    {
        DB::tryLogCurrentException(
            "FastAddPeer",
            fmt::format(
                "Failed when try to restore from checkpoint region_id={} new_peer_id={} {}",
                region_id,
                new_peer_id,
                e.message()));
        return genFastAddPeerResFail(FastAddPeerStatus::OtherError);
    }
    catch (...)
    {
        DB::tryLogCurrentException(
            "FastAddPeer",
            fmt::format(
                "Failed when try to restore from checkpoint region_id={} new_peer_id={}",
                region_id,
                new_peer_id));
        return genFastAddPeerResFail(FastAddPeerStatus::OtherError);
    }
}

void ClearFapSnapshot(EngineStoreServerWrap * server, uint64_t region_id, uint64_t state)
{
    try
    {
        RUNTIME_CHECK_MSG(server->tmt, "TMTContext is null");
        RUNTIME_CHECK_MSG(server->proxy_helper, "proxy_helper is null");
        if (!server->tmt->getContext().getSharedContextDisagg()->isDisaggregatedStorageMode())
            return;
        if (state == 0)
        {
            // Failed
            CheckpointIngestInfo::forciblyClean(
                *(server->tmt),
                server->proxy_helper,
                region_id,
                false,
                CheckpointIngestInfo::CleanReason::ProxyFallback);
        }
        else
        {
            // Succeed
            auto fap_ctx = server->tmt->getContext().getSharedContextDisagg()->fap_context;
            if (fap_ctx)
                fap_ctx->cleanTask(
                    *(server->tmt),
                    server->proxy_helper,
                    region_id,
                    CheckpointIngestInfo::CleanReason::Success);
        }
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}
} // namespace DB

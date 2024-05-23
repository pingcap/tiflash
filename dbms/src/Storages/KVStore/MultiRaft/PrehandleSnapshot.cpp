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

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToDTFilesOutputStream.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

#include <chrono>

namespace CurrentMetrics
{
extern const Metric RaftNumPrehandlingSubTasks;
extern const Metric RaftNumParallelPrehandlingTasks;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char force_set_sst_to_dtfile_block_size[];
extern const char force_set_parallel_prehandle_threshold[];
extern const char force_raise_prehandle_exception[];
extern const char pause_before_prehandle_snapshot[];
extern const char pause_before_prehandle_subtask[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
extern const int REGION_DATA_SCHEMA_UPDATED;
} // namespace ErrorCodes

struct ReadFromStreamResult
{
    PrehandleTransformStatus error = PrehandleTransformStatus::Ok;
    std::string extra_msg;
    RegionPtr region;
};

struct PrehandleTransformCtx
{
    PreHandlingTrace & trace;
    std::shared_ptr<PreHandlingTrace::Item> prehandle_task;
    DM::FileConvertJobType job_type;
    StorageDeltaMergePtr storage;
    const DM::SSTFilesToBlockInputStreamOpts & opts;
    TMTContext & tmt;
};

void PreHandlingTrace::waitForSubtaskResources(uint64_t region_id, size_t parallel, size_t parallel_subtask_limit)
{
    {
        auto current = ongoing_prehandle_subtask_count.load();
        if (current + parallel <= parallel_subtask_limit
            && ongoing_prehandle_subtask_count.compare_exchange_weak(current, current + parallel))
        {
            LOG_DEBUG(
                log,
                "Prehandle resource meet, limit={}, current={}, region_id={}",
                parallel_subtask_limit,
                ongoing_prehandle_subtask_count.load(),
                region_id);
            return;
        }
    }
    Stopwatch watch;
    LOG_DEBUG(
        log,
        "Prehandle resource wait begin, limit={} current={} parallel={} region_id={}",
        parallel_subtask_limit,
        ongoing_prehandle_subtask_count.load(),
        parallel,
        region_id);
    while (true)
    {
        std::unique_lock<std::mutex> cpu_resource_lock{cpu_resource_mut};
        cpu_resource_cv.wait(cpu_resource_lock, [&]() {
            return ongoing_prehandle_subtask_count.load() + parallel <= parallel_subtask_limit;
        });
        auto current = ongoing_prehandle_subtask_count.load();
        if (current + parallel <= parallel_subtask_limit
            && ongoing_prehandle_subtask_count.compare_exchange_weak(current, current + parallel))
        {
            break;
        }
    }
    GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode_parallel_wait)
        .Observe(watch.elapsedSeconds());
    LOG_INFO(
        log,
        "Prehandle resource acquired after {:.3f} seconds, region_id={} parallel={}",
        watch.elapsedSeconds(),
        region_id,
        parallel);
}

static inline std::tuple<ReadFromStreamResult, PrehandleResult> executeTransform(
    LoggerPtr log,
    PrehandleTransformCtx & prehandle_ctx,
    RegionPtr new_region,
    const std::shared_ptr<DM::SSTFilesToBlockInputStream> & sst_stream)
{
    const auto & opts = prehandle_ctx.opts;
    auto & tmt = prehandle_ctx.tmt;
    auto & trace = prehandle_ctx.trace;

    auto region_id = new_region->id();
    auto split_id = sst_stream->getSplitId();
    CurrentMetrics::add(CurrentMetrics::RaftNumPrehandlingSubTasks);
    SCOPE_EXIT({
        trace.releaseSubtaskResources(region_id, split_id);
        CurrentMetrics::sub(CurrentMetrics::RaftNumPrehandlingSubTasks);
    });
    LOG_INFO(
        log,
        "Add prehandle task split_id={} limit={}",
        split_id,
        sst_stream->getSoftLimit().has_value() ? sst_stream->getSoftLimit()->toDebugString() : "");
    std::shared_ptr<DM::SSTFilesToDTFilesOutputStream<DM::BoundedSSTFilesToBlockInputStreamPtr>> stream;
    // If any schema changes is detected during decoding SSTs to DTFiles, we need to cancel and recreate DTFiles with
    // the latest schema. Or we will get trouble in `BoundedSSTFilesToBlockInputStream`.
    try
    {
        auto & context = tmt.getContext();
        auto & global_settings = context.getGlobalContext().getSettingsRef();
        // Read from SSTs and refine the boundary of blocks output to DTFiles
        auto bounded_stream = std::make_shared<DM::BoundedSSTFilesToBlockInputStream>(
            sst_stream,
            ::DB::TiDBPkColumnID,
            opts.schema_snap,
            split_id);

        stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::BoundedSSTFilesToBlockInputStreamPtr>>(
            opts.log_prefix,
            bounded_stream,
            prehandle_ctx.storage,
            opts.schema_snap,
            prehandle_ctx.job_type,
            /* split_after_rows */ global_settings.dt_segment_limit_rows,
            /* split_after_size */ global_settings.dt_segment_limit_size,
            region_id,
            prehandle_ctx.prehandle_task,
            context);

        sst_stream->maybeSkipBySoftLimit();
        stream->writePrefix();
        fiu_do_on(FailPoints::force_raise_prehandle_exception, {
            if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_raise_prehandle_exception); v)
            {
                auto flag = std::any_cast<std::shared_ptr<std::atomic_uint64_t>>(v.value());
                if (flag->load() == 1)
                {
                    LOG_INFO(log, "Throw fake exception once");
                    flag->store(0);
                    throw Exception("fake exception once", ErrorCodes::REGION_DATA_SCHEMA_UPDATED);
                }
                else if (flag->load() == 2)
                {
                    LOG_INFO(log, "Throw fake exception always");
                    throw Exception("fake exception", ErrorCodes::REGION_DATA_SCHEMA_UPDATED);
                }
            }
        });
        FAIL_POINT_PAUSE(FailPoints::pause_before_prehandle_subtask);

        stream->write();
        stream->writeSuffix();
        auto res = ReadFromStreamResult{.error = PrehandleTransformStatus::Ok, .extra_msg = "", .region = new_region};
        auto abort_reason = prehandle_ctx.prehandle_task->abortReason();
        if (abort_reason)
        {
            stream->cancel();
            res = ReadFromStreamResult{.error = abort_reason.value(), .extra_msg = "", .region = new_region};
        }
        return std::make_pair(
            std::move(res),
            PrehandleResult{
                .ingest_ids = stream->outputFiles(),
                .stats = PrehandleResult::Stats{
                    .parallels = 1,
                    .raft_snapshot_bytes = sst_stream->getProcessKeys().total_bytes(),
                    .approx_raft_snapshot_size = 0,
                    .dt_disk_bytes = stream->getTotalBytesOnDisk(),
                    .dt_total_bytes = stream->getTotalCommittedBytes(),
                    .total_keys = sst_stream->getProcessKeys().total(),
                    .write_cf_keys = sst_stream->getProcessKeys().write_cf,
                    .lock_cf_keys = sst_stream->getProcessKeys().lock_cf,
                    .default_cf_keys = sst_stream->getProcessKeys().default_cf,
                    .max_split_write_cf_keys = sst_stream->getProcessKeys().write_cf}});
    }
    catch (DB::Exception & e)
    {
        if (stream != nullptr)
        {
            // Remove all DMFiles.
            stream->cancel();
        }
        if (e.code() == ErrorCodes::REGION_DATA_SCHEMA_UPDATED)
        {
            return std::make_pair(
                ReadFromStreamResult{
                    .error = PrehandleTransformStatus::ErrUpdateSchema,
                    .extra_msg = e.displayText(),
                    .region = new_region},
                PrehandleResult{});
        }
        else if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
        {
            return std::make_pair(
                ReadFromStreamResult{
                    .error = PrehandleTransformStatus::ErrTableDropped,
                    .extra_msg = e.displayText(),
                    .region = new_region},
                PrehandleResult{});
        }
        throw;
    }
}

// It is currently a wrapper for preHandleSSTsToDTFiles.
PrehandleResult KVStore::preHandleSnapshotToFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index,
    TMTContext & tmt)
{
    new_region->beforePrehandleSnapshot(new_region->id(), deadline_index);

    ongoing_prehandle_task_count.fetch_add(1);

    FAIL_POINT_PAUSE(FailPoints::pause_before_prehandle_snapshot);

    uint64_t start_time
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count();
    try
    {
        SCOPE_EXIT({
            auto ongoing = ongoing_prehandle_task_count.fetch_sub(1) - 1;
            new_region->afterPrehandleSnapshot(ongoing);
        });
        PrehandleResult result = preHandleSSTsToDTFiles( //
            new_region,
            snaps,
            index,
            term,
            DM::FileConvertJobType::ApplySnapshot,
            tmt);
        result.stats.start_time = start_time;
        return result;
    }
    catch (DB::Exception & e)
    {
        e.addMessage(
            fmt::format("(while preHandleSnapshot region_id={}, index={}, term={})", new_region->id(), index, term));
        e.rethrow();
    }

    return PrehandleResult{};
}

size_t KVStore::getMaxParallelPrehandleSize() const
{
    const auto & proxy_config = getProxyConfigSummay();
    size_t total_concurrency = 0;
    if (proxy_config.valid)
    {
        total_concurrency = proxy_config.snap_handle_pool_size;
    }
    else
    {
        auto cpu_num = std::thread::hardware_concurrency();
        total_concurrency = static_cast<size_t>(std::clamp(cpu_num * 0.7, 2.0, 16.0));
    }
    return total_concurrency;
}

// If size is 0, do not parallel prehandle for this snapshot, which is regular.
// If size is non-zero, use extra this many threads to prehandle.
static inline std::pair<std::vector<std::string>, size_t> getSplitKey(
    LoggerPtr log,
    KVStore * kvstore,
    RegionPtr new_region,
    std::shared_ptr<DM::SSTFilesToBlockInputStream> sst_stream)
{
    // We don't use this is the single snapshot is small, due to overhead in decoding.
    constexpr size_t default_parallel_prehandle_threshold = 1 * 1024 * 1024 * 1024;
    size_t parallel_prehandle_threshold = default_parallel_prehandle_threshold;
    fiu_do_on(FailPoints::force_set_parallel_prehandle_threshold, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_parallel_prehandle_threshold); v)
            parallel_prehandle_threshold = std::any_cast<size_t>(v.value());
    });

    // Don't change the order of following checks, `getApproxBytes` involves some overhead,
    // although it is optimized to bring about the minimum overhead.
    if constexpr (SERVERLESS_PROXY == 0)
    {
        if (new_region->getClusterRaftstoreVer() != RaftstoreVer::V2)
            return std::make_pair(std::vector<std::string>{}, 0);
    }
    auto approx_bytes = sst_stream->getApproxBytes();
    if (approx_bytes <= parallel_prehandle_threshold)
    {
        LOG_INFO(
            log,
            "getSplitKey refused to split, approx_bytes={} parallel_prehandle_threshold={} region_id={}",
            approx_bytes,
            parallel_prehandle_threshold,
            new_region->id());
        return std::make_pair(std::vector<std::string>{}, approx_bytes);
    }

    // Get this info again, since getApproxBytes maybe take some time.
    auto ongoing_count = kvstore->getOngoingPrehandleTaskCount();
    uint64_t want_split_parts = 0;
    auto total_concurrency = kvstore->getMaxParallelPrehandleSize();
    if (total_concurrency + 1 > ongoing_count)
    {
        // Current thread takes 1 which is in `ongoing_count`.
        // We use all threads to prehandle, since there is potentially no read and no delta merge when prehandling the only region.
        want_split_parts = total_concurrency - ongoing_count + 1;
    }
    if (want_split_parts <= 1)
    {
        LOG_INFO(
            log,
            "getSplitKey refused to split, ongoing={} total_concurrency={} region_id={}",
            ongoing_count,
            total_concurrency,
            new_region->id());
        return std::make_pair(std::vector<std::string>{}, approx_bytes);
    }
    // Will generate at most `want_split_parts - 1` keys.
    std::vector<std::string> split_keys = sst_stream->findSplitKeys(want_split_parts);

    RUNTIME_CHECK_MSG(
        split_keys.size() + 1 <= want_split_parts,
        "findSplitKeys should generate {} - 1 keys, actual {}",
        want_split_parts,
        split_keys.size());
    FmtBuffer fmt_buf;
    if (split_keys.size() + 1 < want_split_parts)
    {
        // If there are too few split keys, the `split_keys` itself may be not be uniformly distributed,
        // it is even better that we still handle it sequantially.
        split_keys.clear();
        LOG_INFO(
            log,
            "getSplitKey failed to split, ongoing={} want={} got={} region_id={}",
            ongoing_count,
            want_split_parts,
            split_keys.size(),
            new_region->id());
    }
    else
    {
        std::sort(split_keys.begin(), split_keys.end());
        fmt_buf.joinStr(
            split_keys.cbegin(),
            split_keys.cend(),
            [](const auto & arg, FmtBuffer & fb) { fb.append(Redact::keyToDebugString(arg.data(), arg.size())); },
            ":");
        LOG_INFO(
            log,
            "getSplitKey result {}, total_concurrency={} ongoing={} total_split_parts={} split_keys={} "
            "region_range={} approx_bytes={} "
            "region_id={}",
            fmt_buf.toString(),
            total_concurrency,
            ongoing_count,
            want_split_parts,
            split_keys.size(),
            new_region->getRange()->toDebugString(),
            approx_bytes,
            new_region->id());
    }
    return std::make_pair(std::move(split_keys), approx_bytes);
}

struct ParallelPrehandleCtx
{
    std::unordered_map<uint64_t, ReadFromStreamResult> gather_res;
    std::unordered_map<uint64_t, PrehandleResult> gather_prehandle_res;
    std::mutex mut;
};
using ParallelPrehandleCtxPtr = std::shared_ptr<ParallelPrehandleCtx>;

static void runInParallel(
    LoggerPtr log,
    PrehandleTransformCtx & prehandle_ctx,
    RegionPtr new_region,
    const SSTViewVec & snaps,
    const TiFlashRaftProxyHelper * proxy_helper,
    uint64_t index,
    uint64_t extra_id,
    ParallelPrehandleCtxPtr parallel_ctx,
    DM::SSTScanSoftLimit && part_limit)
{
    std::string limit_tag = part_limit.toDebugString();
    auto part_new_region = std::make_shared<Region>(new_region->getMeta().clone(), proxy_helper);
    auto part_sst_stream = std::make_shared<DM::SSTFilesToBlockInputStream>(
        part_new_region,
        index,
        snaps,
        proxy_helper,
        prehandle_ctx.tmt,
        std::move(part_limit),
        prehandle_ctx.prehandle_task,
        DM::SSTFilesToBlockInputStreamOpts(prehandle_ctx.opts));
    try
    {
        auto [part_result, part_prehandle_result]
            = executeTransform(log, prehandle_ctx, part_new_region, part_sst_stream);
        LOG_INFO(
            log,
            "Finished extra parallel prehandle task limit {} write_cf={} lock_cf={} default_cf={} dmfiles={} error={}, "
            "split_id={} region_id={}",
            limit_tag,
            part_prehandle_result.stats.write_cf_keys,
            part_prehandle_result.stats.lock_cf_keys,
            part_prehandle_result.stats.default_cf_keys,
            part_prehandle_result.ingest_ids.size(),
            magic_enum::enum_name(part_result.error),
            extra_id,
            part_new_region->id());
        if (part_result.error == PrehandleTransformStatus::ErrUpdateSchema)
        {
            prehandle_ctx.prehandle_task->abortFor(PrehandleTransformStatus::ErrUpdateSchema);
        }
        {
            std::scoped_lock l(parallel_ctx->mut);
            parallel_ctx->gather_res[extra_id] = std::move(part_result);
            parallel_ctx->gather_prehandle_res[extra_id] = std::move(part_prehandle_result);
        }
    }
    catch (Exception & e)
    {
        // Exceptions other than PrehandleTransformStatus.
        // The exception can be wrapped in the future, however, we abort here.
        const auto & processed_keys = part_sst_stream->getProcessKeys();
        LOG_INFO(
            log,
            "Parallel prehandling error {}"
            " write_cf_off={}"
            " split_id={} region_id={}",
            e.message(),
            processed_keys.write_cf,
            extra_id,
            part_new_region->id());
        prehandle_ctx.prehandle_task->abortFor(PrehandleTransformStatus::Aborted);
        throw;
    }
}

void executeParallelTransform(
    LoggerPtr log,
    PrehandleTransformCtx & prehandle_ctx,
    RegionPtr new_region,
    ReadFromStreamResult & result,
    PrehandleResult & prehandle_result,
    const std::vector<std::string> & split_keys,
    std::shared_ptr<DM::SSTFilesToBlockInputStream> sst_stream,
    const SSTViewVec & snaps,
    const TiFlashRaftProxyHelper * proxy_helper,
    uint64_t index)
{
    CurrentMetrics::add(CurrentMetrics::RaftNumParallelPrehandlingTasks);
    SCOPE_EXIT({ CurrentMetrics::sub(CurrentMetrics::RaftNumParallelPrehandlingTasks); });
    using SingleSnapshotAsyncTasks = AsyncTasks<uint64_t, std::function<bool()>, bool>;
    auto split_key_count = split_keys.size();
    RUNTIME_CHECK_MSG(
        split_key_count >= 1,
        "split_key_count should be more or equal than 1, actual {}",
        split_key_count);
    LOG_INFO(
        log,
        "Parallel prehandling for single big region, range={}, split keys={}, region_id={}",
        new_region->getRange()->toDebugString(),
        split_key_count,
        new_region->id());
    Stopwatch watch;
    // Make sure the queue is bigger than `split_key_count`, otherwise `addTask` may fail.
    auto async_tasks = SingleSnapshotAsyncTasks(split_key_count, split_key_count, split_key_count + 5);
    sst_stream->resetSoftLimit(
        DM::SSTScanSoftLimit(DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT, std::string(""), std::string(split_keys[0])));

    ParallelPrehandleCtxPtr parallel_ctx = std::make_shared<ParallelPrehandleCtx>();

    for (size_t extra_id = 0; extra_id < split_key_count; extra_id++)
    {
        auto add_result = async_tasks.addTask(extra_id, [&, extra_id]() {
            auto limit = DM::SSTScanSoftLimit(
                extra_id,
                std::string(split_keys[extra_id]),
                extra_id + 1 == split_key_count ? std::string("") : std::string(split_keys[extra_id + 1]));
            runInParallel(
                log,
                prehandle_ctx,
                new_region,
                snaps,
                proxy_helper,
                index,
                extra_id,
                parallel_ctx,
                std::move(limit));
            return true;
        });
        RUNTIME_CHECK_MSG(
            add_result,
            "Failed when adding {}-th task for prehandling region_id={}",
            extra_id,
            new_region->id());
    }
    // This will read the keys from the beginning to the first split key
    auto [head_result, head_prehandle_result] = executeTransform(log, prehandle_ctx, new_region, sst_stream);
    LOG_INFO(
        log,
        "Finished extra parallel prehandle task limit={} write_cf {} lock_cf={} default_cf={} dmfiles={} "
        "error={}, split_id={}, "
        "region_id={}",
        sst_stream->getSoftLimit()->toDebugString(),
        head_prehandle_result.stats.write_cf_keys,
        head_prehandle_result.stats.lock_cf_keys,
        head_prehandle_result.stats.default_cf_keys,
        head_prehandle_result.ingest_ids.size(),
        magic_enum::enum_name(head_result.error),
        DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT,
        new_region->id());

    // Wait all threads to join. May throw.
    // If one thread throws, then all result is useless, so `async_tasks` is released directly.
    for (size_t extra_id = 0; extra_id < split_key_count; extra_id++)
    {
        // May get exception.
        LOG_DEBUG(log, "Try fetch prehandle task split_id={}, region_id={}", extra_id, new_region->id());
        async_tasks.fetchResult(extra_id);
    }
    if (head_result.error == PrehandleTransformStatus::Ok)
    {
        prehandle_result = std::move(head_prehandle_result);
        // Aggregate results.
        for (size_t extra_id = 0; extra_id < split_key_count; extra_id++)
        {
            std::scoped_lock l(parallel_ctx->mut);
            if (parallel_ctx->gather_res[extra_id].error == PrehandleTransformStatus::Ok)
            {
                result.error = PrehandleTransformStatus::Ok;
                auto & v = parallel_ctx->gather_prehandle_res[extra_id];
                prehandle_result.ingest_ids.insert(
                    prehandle_result.ingest_ids.end(),
                    std::make_move_iterator(v.ingest_ids.begin()),
                    std::make_move_iterator(v.ingest_ids.end()));
                v.ingest_ids.clear();
                prehandle_result.stats.mergeFrom(v.stats);
                // Merge all uncommitted data in different splits.
                new_region->mergeDataFrom(*parallel_ctx->gather_res[extra_id].region);
            }
            else
            {
                // Once a prehandle has non-ok result, we quit further loop
                result = parallel_ctx->gather_res[extra_id];
                result.extra_msg = fmt::format(", from {}", extra_id);
                break;
            }
        }
        LOG_INFO(
            log,
            "Finished all extra parallel prehandle task, write_cf={} dmfiles={} error={} splits={} cost={:.3f}s "
            "region_id={}",
            prehandle_result.stats.write_cf_keys,
            prehandle_result.ingest_ids.size(),
            magic_enum::enum_name(head_result.error),
            split_key_count,
            watch.elapsedSeconds(),
            new_region->id());
    }
    else
    {
        // Otherwise, fallback to error handling or exception handling.
        result = head_result;
        result.extra_msg = fmt::format(", from {}", DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT);
    }
}

/// `preHandleSSTsToDTFiles` read data from SSTFiles and generate DTFile(s) for commited data
/// return the ids of DTFile(s), the uncommitted data will be inserted to `new_region`
PrehandleResult KVStore::preHandleSSTsToDTFiles(
    RegionPtr new_region,
    const SSTViewVec snaps,
    uint64_t index,
    uint64_t term,
    DM::FileConvertJobType job_type,
    TMTContext & tmt)
{
    // if it's only a empty snapshot, we don't create the Storage object, but return directly.
    if (snaps.len == 0)
    {
        return {};
    }
    auto & context = tmt.getContext();
    auto keyspace_id = new_region->getKeyspaceID();
    bool force_decode = false;
    size_t expected_block_size = DEFAULT_MERGE_BLOCK_SIZE;

    // Use failpoint to change the expected_block_size for some test cases
    fiu_do_on(FailPoints::force_set_sst_to_dtfile_block_size, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_sst_to_dtfile_block_size); v)
            expected_block_size = std::any_cast<size_t>(v.value());
    });

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode)
            .Observe(watch.elapsedSeconds());
    });

    PrehandleResult prehandle_result;
    TableID physical_table_id = InvalidTableID;

    auto region_id = new_region->id();
    auto prehandle_task = prehandling_trace.registerTask(region_id);
    while (true)
    {
        // If any schema changes is detected during decoding SSTs to DTFiles, we need to cancel and recreate DTFiles with
        // the latest schema. Or we will get trouble in `BoundedSSTFilesToBlockInputStream`.
        try
        {
            // Get storage schema atomically, will do schema sync if the storage does not exists.
            // Will return the storage even if it is tombstone.
            const auto [table_drop_lock, storage, schema_snap] = AtomicGetStorageSchema(new_region, tmt);
            if (unlikely(storage == nullptr))
            {
                // The storage must be physically dropped, throw exception and do cleanup.
                throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Can't get table");
            }

            // Get a gc safe point for compact filter.
            Timestamp gc_safepoint = 0;
            if (auto pd_client = tmt.getPDClient(); !pd_client->isMock())
            {
                gc_safepoint = PDClientHelper::getGCSafePointWithRetry(
                    pd_client,
                    keyspace_id,
                    /* ignore_cache= */ false,
                    context.getSettingsRef().safe_point_update_interval_seconds);
            }
            physical_table_id = storage->getTableInfo().id;

            auto opt = DM::SSTFilesToBlockInputStreamOpts{
                .log_prefix = fmt::format("keyspace={} table_id={}", keyspace_id, physical_table_id),
                .schema_snap = schema_snap,
                .gc_safepoint = gc_safepoint,
                .force_decode = force_decode,
                .expected_size = expected_block_size};

            auto sst_stream = std::make_shared<DM::SSTFilesToBlockInputStream>(
                new_region,
                index,
                snaps,
                proxy_helper,
                tmt,
                std::nullopt,
                prehandle_task,
                DM::SSTFilesToBlockInputStreamOpts(opt));

            PrehandleTransformCtx prehandle_ctx{
                .trace = prehandling_trace,
                .prehandle_task = prehandle_task,
                .job_type = job_type,
                .storage = storage,
                .opts = opt,
                .tmt = tmt};

            // `split_keys` do not begin with 'z'.
            auto [split_keys, approx_bytes] = getSplitKey(log, this, new_region, sst_stream);
            prehandling_trace.waitForSubtaskResources(region_id, split_keys.size() + 1, getMaxParallelPrehandleSize());
            ReadFromStreamResult result;
            if (split_keys.empty())
            {
                LOG_INFO(
                    log,
                    "Single threaded prehandling for single region, range={} region_id={}",
                    new_region->getRange()->toDebugString(),
                    new_region->id());
                std::tie(result, prehandle_result) = executeTransform(log, prehandle_ctx, new_region, sst_stream);
            }
            else
            {
                executeParallelTransform(
                    log,
                    prehandle_ctx,
                    new_region,
                    result,
                    prehandle_result,
                    split_keys,
                    sst_stream,
                    snaps,
                    proxy_helper,
                    index);
            }

            prehandle_result.stats.approx_raft_snapshot_size = approx_bytes;
            if (result.error == PrehandleTransformStatus::ErrUpdateSchema)
            {
                // It will be thrown in `SSTFilesToBlockInputStream`.
                // The schema of decoding region data has been updated, need to clear and recreate another stream for writing DTFile(s)
                new_region->clearAllData();

                if (force_decode)
                {
                    // Can not decode data with `force_decode == true`, must be something wrong
                    throw Exception(
                        ErrorCodes::REGION_DATA_SCHEMA_UPDATED,
                        fmt::format("Force decode failed {}", result.extra_msg));
                }

                // Update schema and try to decode again
                LOG_INFO(
                    log,
                    "Decoding Region snapshot data meet error, sync schema and try to decode again {} [error={}]",
                    new_region->toString(true),
                    result.extra_msg);
                GET_METRIC(tiflash_schema_trigger_count, type_raft_decode).Increment();
                tmt.getSchemaSyncerManager()->syncTableSchema(context, keyspace_id, physical_table_id);
                // Next time should force_decode
                force_decode = true;
                prehandle_result = PrehandleResult{};
                prehandle_task->reset();

                continue;
            }
            else if (result.error == PrehandleTransformStatus::ErrTableDropped)
            {
                // We can ignore if storage is dropped.
                LOG_INFO(
                    log,
                    "Pre-handle snapshot to DTFiles is ignored because the table is dropped {}",
                    new_region->toString(true));
                break;
            }
            else if (result.error == PrehandleTransformStatus::Aborted)
            {
                LOG_INFO(
                    log,
                    "Apply snapshot is aborted, cancelling. region_id={} term={} index={}",
                    region_id,
                    term,
                    index);
            }

            (void)table_drop_lock; // the table should not be dropped during ingesting file
            break;
        }
        catch (DB::Exception & e)
        {
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            {
                // It will be thrown in many places that will lock a table.
                // We can ignore if storage is dropped.
                LOG_INFO(
                    log,
                    "Pre-handle snapshot to DTFiles is ignored because the table is dropped {}",
                    new_region->toString(true));
                break;
            }
            else
            {
                // Other unrecoverable error, throw
                e.addMessage(fmt::format("keyspace={} physical_table_id={}", keyspace_id, physical_table_id));
                throw;
            }
        }
    } // while

    return prehandle_result;
}

void KVStore::abortPreHandleSnapshot(UInt64 region_id, TMTContext & tmt)
{
    UNUSED(tmt);
    auto prehandle_task = prehandling_trace.deregisterTask(region_id);
    if (prehandle_task)
    {
        // The task is registered, set the cancel flag to true and the generated files
        // will be clear later by `releasePreHandleSnapshot`
        LOG_INFO(log, "Try cancel pre-handling from upper layer, region_id={}", region_id);
        prehandle_task->abortFor(PrehandleTransformStatus::Aborted);
    }
    else
    {
        // the task is not registered, continue
        LOG_INFO(log, "Start cancel pre-handling from upper layer, region_id={}", region_id);
    }
}

template <>
void KVStore::releasePreHandledSnapshot<RegionPtrWithSnapshotFiles>(
    const RegionPtrWithSnapshotFiles & s,
    TMTContext & tmt)
{
    auto & storages = tmt.getStorages();
    auto keyspace_id = s.base->getKeyspaceID();
    auto table_id = s.base->getMappedTableID();
    auto storage = storages.get(keyspace_id, table_id);
    if (storage->engineType() != TiDB::StorageEngine::DT)
    {
        return;
    }
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    LOG_INFO(
        log,
        "Release prehandled snapshot, clean {} dmfiles, region_id={} keyspace={} table_id={}",
        s.external_files.size(),
        s.base->id(),
        keyspace_id,
        table_id);
    auto & context = tmt.getContext();
    dm_storage->cleanPreIngestFiles(s.external_files, context.getSettingsRef());
}

void Region::beforePrehandleSnapshot(uint64_t region_id, std::optional<uint64_t> deadline_index)
{
    if (getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        data.orphan_keys_info.snapshot_index = appliedIndex();
        data.orphan_keys_info.pre_handling = true;
        data.orphan_keys_info.deadline_index = deadline_index;
        data.orphan_keys_info.region_id = region_id;
    }
}

void Region::afterPrehandleSnapshot(int64_t ongoing)
{
    if (getClusterRaftstoreVer() == RaftstoreVer::V2)
    {
        data.orphan_keys_info.pre_handling = false;
        LOG_INFO(
            log,
            "After prehandle, orphan keys remains={} removed={} ongoing_prehandle={} region_id={}",
            data.orphan_keys_info.remainedKeyCount(),
            data.orphan_keys_info.removed_remained_keys.size(),
            ongoing,
            id());
    }
}

} // namespace DB

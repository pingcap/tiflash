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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/RaftLogManager.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <kvproto/raft_serverpb.pb.h>

namespace DB
{

bool RaftLogEagerGcTasks::updateHint(
    RegionID region_id,
    UInt64 eager_truncated_index,
    UInt64 applied_index,
    UInt64 threshold)
{
    if (threshold == 0 //
        || applied_index < eager_truncated_index || applied_index - eager_truncated_index < threshold)
        return false;

    // Try to register a task for eager remove RaftLog to reduce the memory overhead of UniPS
    std::unique_lock lock(mtx_tasks);
    if (auto hint_iter = tasks.find(region_id); hint_iter != tasks.end())
    {
        auto & gc_hint = hint_iter->second;
        if (applied_index > gc_hint.applied_index && applied_index - gc_hint.applied_index >= threshold)
        {
            // LOG_DEBUG(Logger::get(), "update hint, region_id={} applied_index={} hint_applied_index={} gap={}", region_id, applied_index, gc_hint.applied_index, applied_index - gc_hint.applied_index);
            // More updates after last GC task is built, merge it into the current GC task
            gc_hint.eager_truncate_index = std::min(gc_hint.eager_truncate_index, eager_truncated_index);
            gc_hint.applied_index = applied_index;
            return true;
        }
        // No much updates after last GC task is built, let's just keep the task hint unchanged
        return false;
    }

    // New GC task hint is created
    // LOG_DEBUG(Logger::get(), "create hint, region_id={} first_index={} applied_index={}", region_id, first_index, applied_index);
    tasks[region_id] = RaftLogEagerGcHint{
        .eager_truncate_index = eager_truncated_index,
        .applied_index = applied_index,
    };
    return true;
}

RaftLogEagerGcTasks::Hints RaftLogEagerGcTasks::getAndClearHints()
{
    std::unique_lock lock(mtx_tasks);
    Hints hints;
    hints.swap(tasks);
    return hints;
}

namespace details
{
struct RegionGcTask
{
    bool skip;
    UInt64 eager_truncate_index;
    UInt64 applied_index;
};

RegionGcTask getRegionGCTask(
    const RegionID region_id,
    const raft_serverpb::RaftApplyState & region_state,
    const RaftLogEagerGcHint & hint,
    UInt64 threshold,
    const LoggerPtr & logger)
{
    // LOG_DEBUG(logger, "Load persisted region apply state, region_id={} state={}", region_id, region_state.ShortDebugString());
    if (region_state.applied_index() <= 1)
        return RegionGcTask{.skip = true, .eager_truncate_index = 0, .applied_index = 0};

    RegionGcTask task{.skip = false, .eager_truncate_index = 0, .applied_index = 0};
    task.eager_truncate_index = hint.eager_truncate_index;
    task.applied_index = region_state.applied_index() - 1;
    if (task.applied_index <= task.eager_truncate_index || task.applied_index - task.eager_truncate_index <= threshold)
    {
        LOG_DEBUG(
            logger,
            "Skip eager gc, region_id={} persist_first_index={} persist_applied_index={} hint_eager_truncate_index={} "
            "hint_applied_index={}",
            region_id,
            region_state.truncated_state().index(),
            region_state.applied_index(),
            hint.eager_truncate_index,
            hint.applied_index);
        task.skip = true;
    }
    return task;
}

void removeRaftLogs(
    const UniversalPageStoragePtr & uni_ps,
    RegionID region_id,
    UInt64 log_begin,
    UInt64 log_end,
    size_t deletes_per_batch)
{
    UniversalWriteBatch del_batch;
    UInt64 num_deleted = 0;
    for (UInt64 log_index = log_begin; log_index < log_end; ++log_index)
    {
        del_batch.delPage(UniversalPageIdFormat::toRaftLogKey(region_id, log_index));
        num_deleted += 1;
        if (num_deleted % deletes_per_batch == 0)
        {
            uni_ps->write(std::move(del_batch), PageType::RaftData);
            del_batch.clear();
        }
    }

    if (!del_batch.empty())
        uni_ps->write(std::move(del_batch), PageType::RaftData);
}

} // namespace details

RaftLogGcTasksRes executeRaftLogGcTasks(Context & global_ctx, RaftLogEagerGcTasks::Hints && hints)
{
    auto write_node_ps = global_ctx.tryGetWriteNodePageStorage();
    if (!write_node_ps)
        return {};
    assert(write_node_ps);

    const auto eager_gc_rows = global_ctx.getTMTContext().getKVStore()->getRaftLogEagerGCRows();
    LoggerPtr logger = Logger::get();

    Stopwatch watch;
    RaftLogGcTasksRes eager_truncated_indexes;
    size_t num_skip_regions = 0;
    size_t total_num_raft_log_removed = 0;

    RaftDataReader raft_reader(*write_node_ps);
    for (const auto & [region_id, hint] : hints)
    {
        // Read region apply_state from UniPS to ensure that we won't delete any RaftLog that
        // may be read after restart.
        auto region_state = raft_reader.readRegionApplyState(region_id);
        if (!region_state)
        {
            LOG_INFO(logger, "Parse region apply state failed, skip. region_id={}", region_id);
            continue;
        }

        const auto region_task = details::getRegionGCTask(region_id, *region_state, hint, eager_gc_rows, logger);
        if (region_task.skip)
        {
            num_skip_regions += 1;
            continue;
        }

        details::removeRaftLogs(
            write_node_ps,
            region_id,
            region_task.eager_truncate_index,
            region_task.applied_index,
            /*deletes_per_batch*/ 4096);

        // Then we should proceed the eager truncated index to the new applied_index
        eager_truncated_indexes[region_id] = region_task.applied_index;
        size_t num_raft_log_removed = region_task.applied_index - region_task.eager_truncate_index;
        total_num_raft_log_removed += num_raft_log_removed;

        // all logs between applied index and eager truncated index are removed, let's report the gap before removing log
        GET_METRIC(tiflash_raft_raft_log_gap_count, type_eager_gc_applied_index).Observe(num_raft_log_removed);

        LOG_INFO(
            logger,
            "Eager raft log gc, region_id={} eager_truncate_index={} applied_index={} n_removed={} cost={:.3f}s",
            region_id,
            region_task.eager_truncate_index,
            region_task.applied_index,
            num_raft_log_removed,
            watch.elapsedSecondsFromLastTime());
    }

    const double cost_seconds = watch.elapsedSeconds();
    const size_t num_process_regions = hints.size() - num_skip_regions;
    GET_METRIC(tiflash_raft_eager_gc_duration_seconds, type_run).Observe(cost_seconds);
    GET_METRIC(tiflash_raft_eager_gc_count, type_num_raft_logs).Increment(total_num_raft_log_removed);
    GET_METRIC(tiflash_raft_eager_gc_count, type_num_skip_regions).Increment(num_skip_regions);
    GET_METRIC(tiflash_raft_eager_gc_count, type_num_process_regions).Increment(num_process_regions);
    LOG_INFO(
        logger,
        "Eager raft log gc round done, "
        "n_hints={} n_regions={} n_removed_logs={} cost={:.3f}s",
        hints.size(),
        num_process_regions,
        total_num_raft_log_removed,
        cost_seconds);
    return eager_truncated_indexes;
}

} // namespace DB

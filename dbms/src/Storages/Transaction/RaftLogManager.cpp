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
#include <Interpreters/Context.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RaftLogManager.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <kvproto/raft_serverpb.pb.h>

namespace DB
{

bool RaftLogEagerGcTasks::updateHint(RegionID region_id, UInt64 first_index, UInt64 applied_index, UInt64 threshold)
{
    if (applied_index - first_index < threshold)
        return false;

    // Try to register a task for eager remove RaftLog to reduce the memory overhead of UniPS
    std::unique_lock lock(mtx_tasks);
    if (auto hint_iter = tasks.find(region_id); hint_iter != tasks.end())
    {
        if (applied_index > hint_iter->second.applied_index
            && applied_index - hint_iter->second.applied_index >= threshold)
        {
            // LOG_DEBUG(Logger::get(), "update hint, region_id={} applied_index={} hint_applied_index={} diff={}", region_id, applied_index, hint_iter->second.applied_index, applied_index - hint_iter->second.applied_index);
            // task hint is updated
            hint_iter->second.first_index = std::min(hint_iter->second.first_index, first_index);
            hint_iter->second.applied_index = std::max(hint_iter->second.applied_index, applied_index);
            return true;
        }
        // task hint is not changed
        return false;
    }

    // new task hint created
    // LOG_DEBUG(Logger::get(), "create hint, region_id={} first_index={} applied_index={}", region_id, first_index, applied_index);
    tasks[region_id] = RaftLogEagerGcHint{
        .first_index = first_index,
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

struct RegionGcTask
{
    bool skip;
    UInt64 first_index;
    UInt64 applied_index;
};

RegionGcTask getRegionTask(
    const RegionID region_id,
    const raft_serverpb::RaftApplyState & region_state,
    const RaftLogEagerGcHint & hint,
    UInt64 threshold,
    const LoggerPtr & logger)
{
    // LOG_DEBUG(logger, "Load persisted region apply state, region_id={} state={}", region_id, region_state.ShortDebugString());
    if (region_state.applied_index() <= 1)
        return RegionGcTask{.skip = true, .first_index = 0, .applied_index = 0};

    RegionGcTask task{.skip = false, .first_index = 0, .applied_index = 0};
    task.first_index = hint.first_index;
    task.applied_index = region_state.applied_index() - 1;
    if (task.applied_index <= task.first_index || task.applied_index - task.first_index <= threshold)
    {
        LOG_DEBUG(
            logger,
            "Skip eager gc, region_id={} persist_first_index={} persist_applied_index={} hint_first_index={} hint_applied_index={}",
            region_id,
            region_state.truncated_state().index(),
            region_state.applied_index(),
            hint.first_index,
            hint.applied_index);
        task.skip = true;
    }
    return task;
}

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
    size_t num_skip = 0;
    size_t total_num_raft_log_removed = 0;
    for (const auto & [region_id, hint] : hints)
    {
        // Read region apply_state from UniPS
        auto region_apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
        auto page = write_node_ps->read(region_apply_state_key);
        raft_serverpb::RaftApplyState region_apply_state;
        if (!region_apply_state.ParseFromString(String(page.data)))
        {
            LOG_INFO(logger, "Parse region apply state failed, skip. region_id={}", region_id);
            continue;
        }

        UniversalWriteBatch del_batch;
        const auto region_task = getRegionTask(region_id, region_apply_state, hint, eager_gc_rows, logger);
        if (region_task.skip)
        {
            num_skip += 1;
            continue;
        }

        for (UInt64 log_index = region_task.first_index; log_index < region_task.applied_index; ++log_index)
        {
            del_batch.delPage(UniversalPageIdFormat::toRaftLogKey(region_id, log_index));
        }
        write_node_ps->write(std::move(del_batch), PageType::RaftData);

        // Then we should proceed the eager truncated index to the new applied_index
        eager_truncated_indexes[region_id] = region_task.applied_index;
        size_t num_raft_log_removed = region_task.applied_index - region_task.first_index;
        total_num_raft_log_removed += num_raft_log_removed;
        LOG_INFO(
            logger,
            "Eager raft log gc, region_id={} first_index={} applied_index={} n_removed={} cost={:.3f}s",
            region_id,
            region_task.first_index,
            region_task.applied_index,
            num_raft_log_removed,
            watch.elapsedSecondsFromLastTime());
    }
    LOG_INFO(
        logger,
        "Eager raft log gc round done, "
        "n_hints={} n_regions={} n_removed_logs={} cost={:.3f}s",
        hints.size(),
        hints.size() - num_skip,
        total_num_raft_log_removed,
        watch.elapsedSeconds());
    return eager_truncated_indexes;
}

} // namespace DB

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
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Remote/RNSegmentSourceOp.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

#include <magic_enum.hpp>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace DB::FailPoints

namespace DB::DM::Remote
{
void RNSegmentSourceOp::operateSuffixImpl()
{
    LOG_INFO(
        log,
        "Finished reading remote segments, rows={} read_segments={} total_wait_ready_task={:.3f}s total_read={:.3f}s",
        action.totalRows(),
        processed_seg_tasks,
        duration_wait_ready_task_sec,
        duration_read_sec);

    // This metric is per-stream.
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_stream_wait_next_task)
        .Observe(duration_wait_ready_task_sec);
    // This metric is per-stream.
    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_stream_read).Observe(duration_read_sec);
}

void RNSegmentSourceOp::operatePrefixImpl()
{
    workers->startInBackground();
}

ReturnOpStatus RNSegmentSourceOp::startGettingNextReadyTask()
{
    // Start timing the time of get next ready task.
    wait_stop_watch.start();
    // A quick try to get the next task to reduce the overhead of switching to WaitReactor.
    return awaitImpl();
}

ReturnOpStatus RNSegmentSourceOp::readImpl(Block & block)
{
    if unlikely (done)
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }

    if (t_block.has_value())
    {
        std::swap(block, t_block.value());
        action.transform(block, current_seg_task->dm_context->physical_table_id);
        t_block.reset();
        return OperatorStatus::HAS_OUTPUT;
    }

    return current_seg_task ? OperatorStatus::IO_IN : startGettingNextReadyTask();
}

ReturnOpStatus RNSegmentSourceOp::awaitImpl()
{
    if unlikely (done || t_block.has_value())
    {
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        return OperatorStatus::HAS_OUTPUT;
    }

    if unlikely (current_seg_task)
    {
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        return OperatorStatus::IO_IN;
    }

    auto pop_result = workers->getReadyChannel()->tryPop(current_seg_task);
    switch (pop_result)
    {
    case MPMCQueueResult::OK:
        processed_seg_tasks += 1;
        RUNTIME_CHECK(current_seg_task != nullptr);
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        return OperatorStatus::IO_IN;
    case MPMCQueueResult::EMPTY:
        return OperatorStatus::WAITING;
    case MPMCQueueResult::FINISHED:
        current_seg_task = nullptr;
        done = true;
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::CANCELLED:
        current_seg_task = nullptr;
        done = true;
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        throw Exception(workers->getReadyChannel()->getCancelReason());
    default:
        current_seg_task = nullptr;
        done = true;
        duration_wait_ready_task_sec += wait_stop_watch.elapsedSeconds();
        throw Exception(fmt::format("Unexpected pop result {}", magic_enum::enum_name(pop_result)));
    }
}

ReturnOpStatus RNSegmentSourceOp::executeIOImpl()
{
    if unlikely (done || t_block.has_value())
        return OperatorStatus::HAS_OUTPUT;

    if unlikely (!current_seg_task)
        return startGettingNextReadyTask();

    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);
    Block res = current_seg_task->getInputStream()->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();
    if likely (res)
    {
        t_block.emplace(std::move(res));
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        // Current stream is drained, try to get next ready task.
        current_seg_task = nullptr;
        return startGettingNextReadyTask();
    }
}
} // namespace DB::DM::Remote

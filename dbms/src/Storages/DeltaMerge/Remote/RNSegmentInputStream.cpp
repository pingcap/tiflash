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
#include <Common/MPMCQueue.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Remote/RNSegmentInputStream.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>

#include <magic_enum.hpp>

namespace DB::FailPoints
{
extern const char pause_when_reading_from_dt_stream[];
} // namespace DB::FailPoints

namespace DB::DM::Remote
{
RNSegmentInputStream::~RNSegmentInputStream()
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

Block RNSegmentInputStream::readImpl(FilterPtr & res_filter, bool return_filter)
{
    if (done)
        return {};

    workers->startInBackground();

    while (true)
    {
        if (!current_seg_task)
        {
            Stopwatch w{CLOCK_MONOTONIC_COARSE};
            auto pop_result = workers->getReadyChannel()->pop(current_seg_task);
            duration_wait_ready_task_sec += w.elapsedSeconds();

            if (pop_result == MPMCQueueResult::OK)
            {
                processed_seg_tasks += 1;
                RUNTIME_CHECK(current_seg_task != nullptr);
            }
            else if (pop_result == MPMCQueueResult::FINISHED)
            {
                current_seg_task = nullptr;
                done = true;
                return {};
            }
            else if (pop_result == MPMCQueueResult::CANCELLED)
            {
                current_seg_task = nullptr;
                throw Exception(workers->getReadyChannel()->getCancelReason());
            }
            else
            {
                current_seg_task = nullptr;
                RUNTIME_CHECK_MSG(false, "Unexpected pop result {}", magic_enum::enum_name(pop_result));
            }
        }

        Stopwatch w{CLOCK_MONOTONIC_COARSE};
        FAIL_POINT_PAUSE(FailPoints::pause_when_reading_from_dt_stream);
        Block res = current_seg_task->getInputStream()->read(res_filter, return_filter);
        duration_read_sec += w.elapsedSeconds();

        if (!res)
        {
            static constexpr auto inter_type = ConnectionProfileInfo::ConnectionType::InterZoneRemote;
            static constexpr auto inner_type = ConnectionProfileInfo::ConnectionType::InnerZoneRemote;

            RUNTIME_CHECK(connection_profile_infos.size() == 2, connection_profile_infos.size());
            RUNTIME_CHECK(current_seg_task->extra_remote_info.has_value());

            const auto & task_connection_info = current_seg_task->extra_remote_info->connection_profile_info;
            RUNTIME_CHECK(
                task_connection_info.type == inter_type || task_connection_info.type == inner_type,
                task_connection_info.getTypeString());

            if (task_connection_info.type == inter_type)
                connection_profile_infos[INTER_ZONE_INDEX].merge(task_connection_info);
            else if (task_connection_info.type == inner_type)
                connection_profile_infos[INNER_ZONE_INDEX].merge(task_connection_info);

            // Current stream is drained, try read from next stream.
            current_seg_task = nullptr;
            continue;
        }

        if (res.rows() == 0)
        {
            continue;
        }
        else
        {
            action.transform(res, current_seg_task->dm_context->physical_table_id);
            return res;
        }
    }
}

} // namespace DB::DM::Remote

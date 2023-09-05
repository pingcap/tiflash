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

#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkerPrepareStreams.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
}

namespace DB::DM::Remote
{

bool RNWorkerPrepareStreams::initInputStream(const RNReadSegmentTaskPtr & task, bool enable_delta_index_error_fallback)
{
    try
    {
        task->initInputStream(*columns_to_read, read_tso, push_down_filter, read_mode);
        return true;
    }
    catch (const Exception & e)
    {
        if (enable_delta_index_error_fallback && e.code() == ErrorCodes::DT_DELTA_INDEX_ERROR)
        {
            LOG_ERROR(task->meta.segment_snap->log, "{}", e.message());
            return false;
        }
        else
        {
            throw;
        }
    }
}

RNReadSegmentTaskPtr RNWorkerPrepareStreams::doWork(const RNReadSegmentTaskPtr & task)
{
    Stopwatch watch_work{CLOCK_MONOTONIC_COARSE};
    SCOPE_EXIT({
        // This metric is per-segment.
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_worker_prepare_stream)
            .Observe(watch_work.elapsedSeconds());
    });

    if (likely(initInputStream(
            task,
            task->meta.dm_context->db_context.getSettingsRef().dt_enable_delta_index_error_fallback)))
    {
        return task;
    }

    // Exception DT_DELTA_INDEX_ERROR raised. Reset delta index and try again.
    DeltaIndex empty_delta_index;
    task->meta.segment_snap->delta->getSharedDeltaIndex()->swap(empty_delta_index);
    if (auto cache = task->meta.dm_context->db_context.getSharedContextDisagg()->rn_delta_index_cache; cache)
    {
        cache->setDeltaIndex(task->meta.segment_snap->delta->getSharedDeltaIndex());
    }
    initInputStream(task, false);
    return task;
}

} // namespace DB::DM::Remote

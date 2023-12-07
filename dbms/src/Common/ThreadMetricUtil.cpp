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

#include <Common/Stopwatch.h>
#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <common/types.h>

#include <ctime>

std::atomic<UInt64> last_max_thds_metric_reset_ts{0};
const UInt64 max_thds_metric_reset_interval = 60; //60s

namespace DB
{
bool tryToResetMaxThreadsMetrics()
{
    UInt64 now_ts = clock_gettime_ns(CLOCK_MONOTONIC) / 1000000000ULL;
    if (now_ts > last_max_thds_metric_reset_ts + max_thds_metric_reset_interval)
    {
        last_max_thds_metric_reset_ts = now_ts;
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value());
        GET_METRIC(tiflash_thread_count, type_max_active_threads_of_thdpool).Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_thdpool).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_thdpool).Set(GET_METRIC(tiflash_thread_count, type_total_threads_of_thdpool).Value());
        return true;
    }
    return false;
}
} // namespace DB

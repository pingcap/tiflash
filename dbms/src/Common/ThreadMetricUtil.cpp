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

#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <common/types.h>

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
std::atomic<TimePoint> last_max_thds_metric_reset_ts{};
const std::chrono::seconds max_thds_metric_reset_interval{60}; // 60s

namespace DB
{
bool tryToResetMaxThreadsMetrics()
{
    auto last_max_thds_metric_reset_ts_tmp = last_max_thds_metric_reset_ts.load(std::memory_order_relaxed);
    auto now = std::max(Clock::now(), last_max_thds_metric_reset_ts_tmp);
    if (now > last_max_thds_metric_reset_ts_tmp + max_thds_metric_reset_interval)
    {
        last_max_thds_metric_reset_ts.store(now, std::memory_order_relaxed);
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp)
            .Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp)
            .Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw)
            .Set(GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value());
        GET_METRIC(tiflash_thread_count, type_max_active_threads_of_thdpool)
            .Set(GET_METRIC(tiflash_thread_count, type_active_threads_of_thdpool).Value());
        GET_METRIC(tiflash_thread_count, type_max_threads_of_thdpool)
            .Set(GET_METRIC(tiflash_thread_count, type_total_threads_of_thdpool).Value());
        return true;
    }
    return false;
}
} // namespace DB

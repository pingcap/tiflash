#include <Common/Stopwatch.h>
#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <common/types.h>

std::atomic<UInt64> last_max_thds_metric_reset_ts{0};
const UInt64 max_thds_metric_reset_interval = 60; //60s

namespace DB
{
bool tryToResetMaxThreadsMetrics()
{
    UInt64 now_ts = StopWatchDetail::seconds(CLOCK_MONOTONIC);
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
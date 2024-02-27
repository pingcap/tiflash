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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/MemoryTracker.h>
#include <Common/TiFlashMetrics.h>
#include <Common/formatReadable.h>
#include <IO/WriteHelpers.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <iomanip>

namespace CurrentMetrics
{
extern const Metric MemoryTrackingQueryStorageTask;
extern const Metric MemoryTrackingFetchPages;
extern const Metric MemoryTrackingSharedColumnData;
extern const Metric MemoryTrackingKVStore;
} // namespace CurrentMetrics

std::atomic<Int64> real_rss{0}, proc_num_threads{1}, baseline_of_query_mem_tracker{0};
std::atomic<UInt64> proc_virt_size{0};
MemoryTracker::~MemoryTracker()
{
    // Destruction of global root mem tracker means the process is shutting down, log and metrics models may have been released!
    // So we just skip operations of log or metrics for global root mem trackers.
    if (is_global_root)
        return;

    if (peak && log_peak_memory_usage_in_destructor)
    {
        try
        {
            logPeakMemoryUsage();
        }
        catch (...)
        {
            /// Exception in Poco::Logger, intentionally swallow.
        }
    }

    /** This is needed for next memory tracker to be consistent with sum of all referring memory trackers.
      *
      * Sometimes, memory tracker could be destroyed before memory was freed, and on destruction, amount > 0.
      * For example, a query could allocate some data and leave it in cache.
      *
      * If memory will be freed outside of context of this memory tracker,
      *  but in context of one of the 'next' memory trackers,
      *  then memory usage of 'next' memory trackers will be underestimated,
      *  because amount will be decreased twice (first - here, second - when real 'free' happens).
      */
    // TODO In future, maybe we can find a better way to handle the "amount > 0" case.
    if (auto value = amount.load(std::memory_order_relaxed))
        free(value);
}

static Poco::Logger * getLogger()
{
    static Poco::Logger * logger = &Poco::Logger::get("MemoryTracker");
    return logger;
}

static String storageMemoryUsageDetail()
{
    return fmt::format(
        "non-query: peak={}, amount={}; "
        "kvstore: peak={}, amount={}; "
        "query-storage-task: peak={}, amount={}; "
        "fetch-pages: peak={}, amount={}; "
        "shared-column-data: peak={}, amount={}.",
        root_of_non_query_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_non_query_mem_trackers->getPeak())
                                       : "0",
        root_of_non_query_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_non_query_mem_trackers->get())
                                       : "0",
        root_of_kvstore_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_kvstore_mem_trackers->getPeak())
                                     : "0",
        root_of_kvstore_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_kvstore_mem_trackers->get()) : "0",
        sub_root_of_query_storage_task_mem_trackers
            ? formatReadableSizeWithBinarySuffix(sub_root_of_query_storage_task_mem_trackers->getPeak())
            : "0",
        sub_root_of_query_storage_task_mem_trackers
            ? formatReadableSizeWithBinarySuffix(sub_root_of_query_storage_task_mem_trackers->get())
            : "0",
        fetch_pages_mem_tracker ? formatReadableSizeWithBinarySuffix(fetch_pages_mem_tracker->getPeak()) : "0",
        fetch_pages_mem_tracker ? formatReadableSizeWithBinarySuffix(fetch_pages_mem_tracker->get()) : "0",
        shared_column_data_mem_tracker ? formatReadableSizeWithBinarySuffix(shared_column_data_mem_tracker->getPeak())
                                       : "0",
        shared_column_data_mem_tracker ? formatReadableSizeWithBinarySuffix(shared_column_data_mem_tracker->get())
                                       : "0");
}

void MemoryTracker::logPeakMemoryUsage() const
{
    const char * tmp_decr = description.load();
    LOG_DEBUG(
        getLogger(),
        "Peak memory usage{}: {}.",
        (tmp_decr ? " " + std::string(tmp_decr) : ""),
        formatReadableSizeWithBinarySuffix(peak));
}

void MemoryTracker::alloc(Int64 size, bool check_memory_limit)
{
    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);
    reportAmount();

    if (!next.load(std::memory_order_relaxed))
    {
        CurrentMetrics::add(metric, size);
        // Only add shared column data size to root_of_query_mem_trackers.
        if (shared_column_data_mem_tracker && root_of_query_mem_trackers.get() == this)
            will_be += shared_column_data_mem_tracker->get();
    }

    if (check_memory_limit)
    {
        Int64 current_limit = limit.load(std::memory_order_relaxed);
        Int64 current_accuracy_diff_for_test = accuracy_diff_for_test.load(std::memory_order_relaxed);
        if (unlikely(
                !next.load(std::memory_order_relaxed) && current_accuracy_diff_for_test && current_limit
                && real_rss > current_accuracy_diff_for_test + current_limit))
        {
            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory tracker accuracy ");
            const char * tmp_decr = description.load();
            if (tmp_decr)
                fmt_buf.fmtAppend(" {}", tmp_decr);

            fmt_buf.fmtAppend(
                ": fault injected. real_rss ({}) is much larger than limit ({}). Debug info, threads of process: {}, "
                "memory usage tracked by ProcessList: peak {}, current {}. Virtual memory size: {}.",
                formatReadableSizeWithBinarySuffix(real_rss),
                formatReadableSizeWithBinarySuffix(current_limit),
                proc_num_threads.load(),
                (root_of_query_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_query_mem_trackers->peak)
                                            : "0"),
                (root_of_query_mem_trackers ? formatReadableSizeWithBinarySuffix(root_of_query_mem_trackers->amount)
                                            : "0"),
                proc_virt_size.load());
            fmt_buf.fmtAppend(" Memory usage of storage: {}", storageMemoryUsageDetail());
            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }

        /// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
        /// In this case, it doesn't matter.
        if (unlikely(fault_probability && drand48() < fault_probability))
        {
            amount.fetch_sub(size, std::memory_order_relaxed);
            reportAmount();

            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory tracker");
            const char * tmp_decr = description.load();
            if (tmp_decr)
                fmt_buf.fmtAppend(" {}", tmp_decr);
            fmt_buf.fmtAppend(
                ": fault injected. Would use {} (attempt to allocate chunk of {} bytes), maximum: {}.",
                formatReadableSizeWithBinarySuffix(will_be),
                size,
                formatReadableSizeWithBinarySuffix(current_limit));
            fmt_buf.fmtAppend(" Memory Usage of Storage: {}", storageMemoryUsageDetail());
            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }
        Int64 current_bytes_rss_larger_than_limit = bytes_rss_larger_than_limit.load(std::memory_order_relaxed);
        bool is_rss_too_large
            = (!next.load(std::memory_order_relaxed) && current_limit
               && real_rss > current_limit + current_bytes_rss_larger_than_limit
               && will_be > baseline_of_query_mem_tracker);
        if (is_rss_too_large || unlikely(current_limit && will_be > current_limit))
        {
            DB::GET_METRIC(tiflash_memory_exceed_quota_count).Increment();
            amount.fetch_sub(size, std::memory_order_relaxed);
            reportAmount();

            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory limit");
            const char * tmp_decr = description.load();
            if (tmp_decr)
                fmt_buf.fmtAppend(" {}", tmp_decr);

            if (!is_rss_too_large)
            { // out of memory quota
                fmt_buf.fmtAppend(
                    " exceeded caused by 'out of memory quota for data computing' : would use {} for data computing "
                    "(attempt to allocate chunk of {} bytes), limit of memory for data computing: {}.",
                    formatReadableSizeWithBinarySuffix(will_be),
                    size,
                    formatReadableSizeWithBinarySuffix(current_limit));
            }
            else
            { // RSS too large
                fmt_buf.fmtAppend(
                    " exceeded caused by 'RSS(Resident Set Size) much larger than limit' : process memory size would "
                    "be {} for (attempt to allocate chunk of {} bytes), limit of memory for data computing : {}.",
                    formatReadableSizeWithBinarySuffix(real_rss),
                    size,
                    formatReadableSizeWithBinarySuffix(current_limit));
            }

            fmt_buf.fmtAppend(" Memory Usage of Storage: {}", storageMemoryUsageDetail());

            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }
    }

    if (will_be > peak.load(std::memory_order_relaxed)) /// Races doesn't matter. Could rewrite with CAS, but not worth.
        peak.store(will_be, std::memory_order_relaxed);

    if (auto * loaded_next = next.load(std::memory_order_relaxed))
    {
        try
        {
            loaded_next->alloc(size, check_memory_limit);
        }
        catch (...)
        {
            amount.fetch_sub(size, std::memory_order_relaxed);
            reportAmount();
            std::rethrow_exception(std::current_exception());
        }
    }
}


void MemoryTracker::free(Int64 size)
{
    Int64 new_amount = amount.fetch_sub(size, std::memory_order_relaxed) - size;
    reportAmount();

    /** Sometimes, query could free some data, that was allocated outside of query context.
      * Example: cache eviction.
      * To avoid negative memory usage, we "saturate" amount.
      * Memory usage will be calculated with some error.
      * NOTE The code is not atomic. Not worth to fix.
      */
    if (new_amount < 0 && !next.load(std::memory_order_relaxed)) // handle it only for root memory_tracker
    {
        amount.fetch_sub(new_amount);
        reportAmount();
        size += new_amount;
    }

    if (auto * loaded_next = next.load(std::memory_order_relaxed))
        loaded_next->free(size);
    else
        CurrentMetrics::sub(metric, size);
}


void MemoryTracker::reset()
{
    if (!next.load(std::memory_order_relaxed))
        CurrentMetrics::sub(metric, amount.load(std::memory_order_relaxed));

    amount.store(0, std::memory_order_relaxed);
    peak.store(0, std::memory_order_relaxed);
    limit.store(0, std::memory_order_relaxed);
    reportAmount();
}


void MemoryTracker::setOrRaiseLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = limit.load(std::memory_order_relaxed);
    while (old_value < value && !limit.compare_exchange_weak(old_value, value))
        ;
}

void MemoryTracker::reportAmount()
{
    if (amount_metric.has_value())
        CurrentMetrics::set(*amount_metric, amount.load(std::memory_order_relaxed));
}

#if __APPLE__ && __clang__
__thread MemoryTracker * current_memory_tracker = nullptr;
#else
thread_local MemoryTracker * current_memory_tracker = nullptr;
#endif

std::shared_ptr<MemoryTracker> root_of_non_query_mem_trackers = MemoryTracker::createGlobalRoot();
std::shared_ptr<MemoryTracker> root_of_query_mem_trackers = MemoryTracker::createGlobalRoot();
std::shared_ptr<MemoryTracker> root_of_kvstore_mem_trackers = MemoryTracker::createGlobalRoot();

std::shared_ptr<MemoryTracker> sub_root_of_query_storage_task_mem_trackers;
std::shared_ptr<MemoryTracker> fetch_pages_mem_tracker;
std::shared_ptr<MemoryTracker> shared_column_data_mem_tracker;

void initStorageMemoryTracker(Int64 limit, Int64 larger_than_limit)
{
    LOG_INFO(
        getLogger(),
        "Storage task memory limit={}, larger_than_limit={}",
        formatReadableSizeWithBinarySuffix(limit),
        formatReadableSizeWithBinarySuffix(larger_than_limit));

    // When these (static) mem tracker are reset, it means the process is shutdown and the logging system is stopped.
    // Do not log down the usage info in dctor of them.
    bool log_in_destructor = false;
    RUNTIME_CHECK(sub_root_of_query_storage_task_mem_trackers == nullptr);
    sub_root_of_query_storage_task_mem_trackers = MemoryTracker::create(limit, nullptr, log_in_destructor);
    sub_root_of_query_storage_task_mem_trackers->setBytesThatRssLargerThanLimit(larger_than_limit);
    sub_root_of_query_storage_task_mem_trackers->setAmountMetric(CurrentMetrics::MemoryTrackingQueryStorageTask);

    RUNTIME_CHECK(fetch_pages_mem_tracker == nullptr);
    fetch_pages_mem_tracker
        = MemoryTracker::create(0, sub_root_of_query_storage_task_mem_trackers.get(), log_in_destructor);
    fetch_pages_mem_tracker->setAmountMetric(CurrentMetrics::MemoryTrackingFetchPages);

    RUNTIME_CHECK(shared_column_data_mem_tracker == nullptr);
    shared_column_data_mem_tracker
        = MemoryTracker::create(0, sub_root_of_query_storage_task_mem_trackers.get(), log_in_destructor);
    shared_column_data_mem_tracker->setAmountMetric(CurrentMetrics::MemoryTrackingSharedColumnData);

    root_of_kvstore_mem_trackers->setAmountMetric(CurrentMetrics::MemoryTrackingKVStore);
}

namespace CurrentMemoryTracker
{
static Int64 MEMORY_TRACER_SUBMIT_THRESHOLD = 1024 * 1024; // 1 MiB
#if __APPLE__ && __clang__
static __thread Int64 local_delta{};
#else
static thread_local Int64 local_delta{};
#endif

__attribute__((always_inline)) inline void checkSubmitAndUpdateLocalDelta(Int64 updated_local_delta)
{
    if (current_memory_tracker)
    {
        if (unlikely(updated_local_delta > MEMORY_TRACER_SUBMIT_THRESHOLD))
        {
            current_memory_tracker->alloc(updated_local_delta);
            local_delta = 0;
        }
        else if (unlikely(updated_local_delta < -MEMORY_TRACER_SUBMIT_THRESHOLD))
        {
            current_memory_tracker->free(-updated_local_delta);
            local_delta = 0;
        }
        else
        {
            local_delta = updated_local_delta;
        }
    }
}

void disableThreshold()
{
    MEMORY_TRACER_SUBMIT_THRESHOLD = 0;
}

void submitLocalDeltaMemory()
{
    if (current_memory_tracker)
    {
        try
        {
            if (local_delta > 0)
            {
                current_memory_tracker->alloc(local_delta, false);
            }
            else if (local_delta < 0)
            {
                current_memory_tracker->free(-local_delta);
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("MemoryTracker", "Failed when try to submit local delta memory");
        }
    }
    local_delta = 0;
}

Int64 getLocalDeltaMemory()
{
    return local_delta;
}

void alloc(Int64 size)
{
    checkSubmitAndUpdateLocalDelta(local_delta + size);
}

void realloc(Int64 old_size, Int64 new_size)
{
    checkSubmitAndUpdateLocalDelta(local_delta + (new_size - old_size));
}

void free(Int64 size)
{
    checkSubmitAndUpdateLocalDelta(local_delta - size);
}

} // namespace CurrentMemoryTracker

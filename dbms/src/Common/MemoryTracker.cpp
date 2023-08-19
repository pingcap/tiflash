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
#include <Common/formatReadable.h>
#include <IO/WriteHelpers.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <iomanip>

MemoryTracker::~MemoryTracker()
{
    if (peak)
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
    if (auto value = amount.load(std::memory_order_relaxed))
        free(value);
}

static Poco::Logger * getLogger()
{
    static Poco::Logger * logger = &Poco::Logger::get("MemoryTracker");
    return logger;
}

void MemoryTracker::logPeakMemoryUsage() const
{
    LOG_FMT_DEBUG(getLogger(), "Peak memory usage{}: {}.", (description ? " " + std::string(description) : ""), formatReadableSizeWithBinarySuffix(peak));
}

void MemoryTracker::alloc(Int64 size, bool check_memory_limit)
{
    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);

    if (!next.load(std::memory_order_relaxed))
        CurrentMetrics::add(metric, size);

    if (check_memory_limit)
    {
        Int64 current_limit = limit.load(std::memory_order_relaxed);

        /// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
        /// In this case, it doesn't matter.
        if (unlikely(fault_probability && drand48() < fault_probability))
        {
            free(size);

            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory tracker");
            if (description)
                fmt_buf.fmtAppend(" {}", description);
            fmt_buf.fmtAppend(": fault injected. Would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                              formatReadableSizeWithBinarySuffix(will_be),
                              size,
                              formatReadableSizeWithBinarySuffix(current_limit));

            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }

        if (unlikely(current_limit && will_be > current_limit))
        {
            free(size);

            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory limit");
            if (description)
                fmt_buf.fmtAppend(" {}", description);

            fmt_buf.fmtAppend(" exceeded: would use {} (attempt to allocate chunk of {} bytes), maximum: {}",
                              formatReadableSizeWithBinarySuffix(will_be),
                              size,
                              formatReadableSizeWithBinarySuffix(current_limit));

            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }
    }

    if (will_be > peak.load(std::memory_order_relaxed)) /// Races doesn't matter. Could rewrite with CAS, but not worth.
        peak.store(will_be, std::memory_order_relaxed);

    if (auto * loaded_next = next.load(std::memory_order_relaxed))
        loaded_next->alloc(size, check_memory_limit);
}


void MemoryTracker::free(Int64 size)
{
    Int64 new_amount = amount.fetch_sub(size, std::memory_order_relaxed) - size;

    /** Sometimes, query could free some data, that was allocated outside of query context.
      * Example: cache eviction.
      * To avoid negative memory usage, we "saturate" amount.
      * Memory usage will be calculated with some error.
      * NOTE The code is not atomic. Not worth to fix.
      */
    if (new_amount < 0)
    {
        amount.fetch_sub(new_amount);
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
}


void MemoryTracker::setOrRaiseLimit(Int64 value)
{
    /// This is just atomic set to maximum.
    Int64 old_value = limit.load(std::memory_order_relaxed);
    while (old_value < value && !limit.compare_exchange_weak(old_value, value))
        ;
}

#if __APPLE__ && __clang__
__thread MemoryTracker * current_memory_tracker = nullptr;
#else
thread_local MemoryTracker * current_memory_tracker = nullptr;
#endif

namespace CurrentMemoryTracker
{
static Int64 MEMORY_TRACER_SUBMIT_THRESHOLD = 8 * 1024 * 1024; // 8 MiB
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

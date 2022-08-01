// Copyright 2022 PingCAP, Ltd.
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
#include <unordered_set>

#include <iomanip>
std::atomic<long long> dirty_alloc{0}, dirty_free{0}, alct_cnt{0}, alct_sum{0}, max_alct{0};
std::atomic<long long> tracked_mem{0}, mt_tracked_mem{0}, tracked_peak{0}, untracked_mem{0}, tot_local_delta{0}, tracked_mem_p2{0}, tracked_mem_t3{0};
std::atomic<long long> tracked_alloc{0}, tracked_reloc{0}, tracked_free{0}, tracked_alct{0};
std::atomic<long long> tracked_rec_alloc{0}, tracked_rec_reloc{0}, tracked_rec_free{0};
std::unordered_set<MemoryTracker*> root_memtracker_set;
std::mutex rms_mu;
MemoryTracker* proc_memory_tracker = nullptr;
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

bool MemoryTracker::alloc(Int64 size, bool check_memory_limit)
{
    /** Using memory_order_relaxed means that if allocations are done simultaneously,
      *  we allow exception about memory limit exceeded to be thrown only on next allocation.
      * So, we allow over-allocations.
      */
    if (!next.load()) {
        mt_tracked_mem += size;
    }
    if (proc_memory_tracker == this) {
        // auto * loaded_next = this;
        // bool has_desired_root = false;
        // do {
        //     if (loaded_next == proc_memory_tracker) {
        //         has_desired_root = true;
        //         break;
        //     }
        // } while ((loaded_next = next.load(std::memory_order_relaxed)));
        // if (!has_desired_root) {
        //     untracked_mem += size;
        // }
    }
    // if (!next.load() && proc_memory_tracker != this) {
    //     untracked_mem += size;
    // }
    // {
    //     auto pre_loaded_next = this;
    //     auto * loaded_next = next.load(std::memory_order_relaxed);   
    //     while (loaded_next) {
    //         pre_loaded_next = loaded_next;
    //         loaded_next = next.load(std::memory_order_relaxed);
    //     }
    //     // int root_sz = 1;
    //     {
    //         std::unique_lock lk(rms_mu);
    //         root_memtracker_set.insert(pre_loaded_next);
    //         // root_sz = root_memtracker_set.size();
    //     }
    // }

    //TODO revert
    Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);
    // Int64 will_be = size + amount.fetch_add(size);
    if (!next.load(std::memory_order_relaxed))
        CurrentMetrics::add(metric, size);

    if (check_memory_limit)
    {
        Int64 current_limit = limit.load(std::memory_order_relaxed);

        /// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
        /// In this case, it doesn't matter.
        if (unlikely(fault_probability && drand48() < fault_probability))
        {
            //TODO revert
            // free(size);
            amount.fetch_sub(size, std::memory_order_relaxed);

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
        //TODO revert "tracked_mem > current_limit  ||"
        if (
            //  (current_limit && tracked_mem.load() > current_limit)  ||
         unlikely(current_limit && will_be > current_limit))
        {
            auto root_amount = amount.load(std::memory_order_relaxed);
            auto pre_loaded_next = this;
            auto * loaded_next = next.load(std::memory_order_relaxed);   
            while (loaded_next) {
                root_amount = loaded_next->amount.load(std::memory_order_relaxed);
                // loaded_next->alloc(size, check_memory_limit);
                pre_loaded_next = loaded_next;
                loaded_next = next.load(std::memory_order_relaxed);
                // if (!tmp_loaded_next) {
                //     std::unique_lock lk(rms_mu);
                //     root_memtracker_set.insert(loaded_next);
                // }

            }
            int root_sz = 1;
            {
                std::unique_lock lk(rms_mu);
                root_memtracker_set.insert(pre_loaded_next);
                root_sz = root_memtracker_set.size();
            }
                
            //TODO revert
            // free(size);
            amount.fetch_sub(size, std::memory_order_relaxed);

            DB::FmtBuffer fmt_buf;
            fmt_buf.append("Memory limit");
            if (description)
                fmt_buf.fmtAppend(" {}", description);

            fmt_buf.fmtAppend(" exceeded: would use {} (attempt to allocate chunk of {} bytes), maximum: {}, amount:{}, tracked_mem: {}, tracked_memp2: {}, tracked_mem_t3:{}, root_amout: {}, untracked_mem: {}, root_cnt:{}, tot_local_delta: {}, mt_tracked_mem:{}",
                              formatReadableSizeWithBinarySuffix(will_be),
                              size,
                              formatReadableSizeWithBinarySuffix(current_limit),
                              formatReadableSizeWithBinarySuffix(amount.load(std::memory_order_relaxed)),
                              formatReadableSizeWithBinarySuffix(tracked_mem.load()),
                              formatReadableSizeWithBinarySuffix(tracked_mem_p2.load()),
                              formatReadableSizeWithBinarySuffix(tracked_mem_t3.load()),
                              formatReadableSizeWithBinarySuffix(root_amount),
                              formatReadableSizeWithBinarySuffix(untracked_mem.load()),
                              root_sz,
                              formatReadableSizeWithBinarySuffix(tot_local_delta.load()),
                              formatReadableSizeWithBinarySuffix(mt_tracked_mem.load())
                              );

            throw DB::TiFlashException(fmt_buf.toString(), DB::Errors::Coprocessor::MemoryLimitExceeded);
        }
    }

    if (will_be > peak.load(std::memory_order_relaxed)) /// Races doesn't matter. Could rewrite with CAS, but not worth.
        peak.store(will_be, std::memory_order_relaxed);

    if (auto * loaded_next = next.load(std::memory_order_relaxed)) {
        bool fg = false;
        try {
            fg = loaded_next->alloc(size, check_memory_limit);
        } catch (...) {
            amount.fetch_sub(size, std::memory_order_relaxed);
            std::rethrow_exception(std::current_exception());
        }
        return fg || proc_memory_tracker == this;
    }
    return proc_memory_tracker == this;
}


bool MemoryTracker::free(Int64 size)
{
    if (!next.load()) {
        mt_tracked_mem -= size;
    }
    if (proc_memory_tracker == this) {
        // auto * loaded_next = this;
        // bool has_desired_root = false;
        // do {
        //     if (loaded_next == proc_memory_tracker) {
        //         has_desired_root = true;
        //         break;
        //     }
        // } while ((loaded_next = next.load(std::memory_order_relaxed)));
        // if (!has_desired_root) {
        //     untracked_mem -= size;
        // }
    }
    // if (!next.load() && proc_memory_tracker != this) {
    //     untracked_mem += size;
    // }
    
    //TODO revert
    //Int64 new_amount = amount.fetch_sub(size, std::memory_order_relaxed) - size;
    Int64 new_amount = amount.fetch_sub(size, std::memory_order_relaxed) - size;
    /** Sometimes, query could free some data, that was allocated outside of query context.
      * Example: cache eviction.
      * To avoid negative memory usage, we "saturate" amount.
      * Memory usage will be calculated with some error.
      * NOTE The code is not atomic. Not worth to fix.
      */

      //TODO revert
    // if (new_amount < 0)
    // {
    //     amount.fetch_sub(new_amount);
    //     size += new_amount;
    // }

    if (auto * loaded_next = next.load(std::memory_order_relaxed)) {
        bool fg = loaded_next->free(size);
        return fg || proc_memory_tracker == this;
    }
    else {
        CurrentMetrics::sub(metric, size);
        return proc_memory_tracker == this;
    }

}


void MemoryTracker::reset()
{
    if (!next.load(std::memory_order_relaxed))
        CurrentMetrics::sub(metric, amount.load(std::memory_order_relaxed));

    amount.store(0);
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
static Int64 MEMORY_TRACER_SUBMIT_THRESHOLD = 0; // 8 MiB /// TODO revert 
#if __APPLE__ && __clang__
static __thread Int64 local_delta{};
#else
static thread_local Int64 local_delta{};
#endif

__attribute__((always_inline)) inline bool checkSubmitAndUpdateLocalDelta(Int64 updated_local_delta)
{
    if (current_memory_tracker)
    {
        if (unlikely(updated_local_delta > MEMORY_TRACER_SUBMIT_THRESHOLD))
        {
            
            if (!current_memory_tracker->alloc(updated_local_delta)) {
                untracked_mem += updated_local_delta;
            } else {
                tracked_mem_t3 += updated_local_delta;
            }
            tot_local_delta -= local_delta; // DEBUG tot_local_delta
            local_delta = 0;
            return true;
        }
        else if (unlikely(updated_local_delta < -MEMORY_TRACER_SUBMIT_THRESHOLD))
        {
            if (!current_memory_tracker->free(-updated_local_delta)) {
                untracked_mem -= -updated_local_delta;
            } else {
                tracked_mem_t3 -= -updated_local_delta;
            }
            tot_local_delta -= local_delta; // DEBUG tot_local_delta
            local_delta = 0;
            return true;
        }
        else
        {
            tot_local_delta += (updated_local_delta-local_delta); // DEBUG tot_local_delta
            local_delta = updated_local_delta;
            return false;
        }
    }
    return false;
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
                if (!current_memory_tracker->alloc(local_delta, false)) {
                    untracked_mem += local_delta;
                }
            }
            else if (local_delta < 0)
            {
                if (!current_memory_tracker->free(-local_delta)) {
                    untracked_mem -= -local_delta;
                }
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("MemoryTracker", "Failed when try to submit local delta memory");
        }
    }
    tot_local_delta -= local_delta;
    local_delta = 0;
}

Int64 getLocalDeltaMemory()
{
    return local_delta;
}

void alloc(Int64 size)
{
    if (current_memory_tracker)
    {
        tracked_rec_alloc++;
        if (!checkSubmitAndUpdateLocalDelta(local_delta + size)) {
            untracked_mem += size;
        } else {
            // tracked_mem_t3 += size;
        }
    }
    else
    {
        untracked_mem += size;
        tracked_alloc++;
    }
    tracked_mem += size;
    tracked_mem_p2 += size;
    long long cur_mem = tracked_mem;
    if (cur_mem > tracked_peak) {
        tracked_peak = cur_mem;
    }
}

void realloc(Int64 old_size, Int64 new_size)
{
    
    if (current_memory_tracker)
    {
        tracked_rec_reloc++;
        if (!checkSubmitAndUpdateLocalDelta(local_delta + (new_size - old_size))) {
            untracked_mem += new_size - old_size;
        } else {
            // tracked_mem_t3 += new_size - old_size;
        }
    }
    else
    {
        tracked_reloc++;
        untracked_mem += new_size - old_size;
    }
    tracked_mem += (new_size - old_size);
    tracked_mem_p2 += (new_size - old_size);
}

void free(Int64 size)
{
    if (current_memory_tracker)
    {
        tracked_rec_free++;
        if (!checkSubmitAndUpdateLocalDelta(local_delta - size)) {
            untracked_mem -= size;
        } else {
            // tracked_mem_t3 -= size;
        }
    }
    else
    {
        untracked_mem -= size;
        tracked_free++;
        
    }
    tracked_mem -= size;
    tracked_mem_p2 -= size;
}

} // namespace CurrentMemoryTracker

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

#pragma once

#include <Common/CurrentMetrics.h>
#include <common/types.h>

#include <atomic>

extern std::atomic<Int64> real_rss;
namespace CurrentMetrics
{
extern const Metric MemoryTracking;
}

class MemoryTracker;
using MemoryTrackerPtr = std::shared_ptr<MemoryTracker>;

/** Tracks memory consumption.
  * It throws an exception if amount of consumed memory become greater than certain limit.
  * The same memory tracker could be simultaneously used in different threads.
  */
class MemoryTracker : public std::enable_shared_from_this<MemoryTracker>
{
    std::atomic<Int64> amount{0};
    std::atomic<Int64> peak{0};
    std::atomic<Int64> limit{0};

    // How many bytes RSS(Resident Set Size) can be larger than limit(max_memory_usage_for_all_queries). Default: 5GB
    Int64 bytes_rss_larger_than_limit = 5368709120;

    /// To test exception safety of calling code, memory tracker throws an exception on each memory allocation with specified probability.
    double fault_probability = 0;

    /// To test the accuracy of memory track, it throws an exception when the part exceeding the tracked amount is greater than accuracy_diff_for_test.
    Int64 accuracy_diff_for_test = 0;

    /// Singly-linked list. All information will be passed to subsequent memory trackers also (it allows to implement trackers hierarchy).
    /// In terms of tree nodes it is the list of parents. Lifetime of these trackers should "include" lifetime of current tracker.
    std::atomic<MemoryTracker *> next{};
    std::shared_ptr<MemoryTracker> next_holder; // hold this to prevent next Memtracker from being released

    /// You could specify custom metric to track memory usage.
    CurrentMetrics::Metric metric = CurrentMetrics::MemoryTracking;

    /// This description will be used as prefix into log messages (if isn't nullptr)
    const char * description = nullptr;

    /// Make constructors private to ensure all objects of this class is created by `MemoryTracker::create`.
    MemoryTracker() = default;
    explicit MemoryTracker(Int64 limit_)
        : limit(limit_)
    {}

public:
    /// Using `std::shared_ptr` and `new` instread of `std::make_shared` is because `std::make_shared` cannot call private constructors.
    static MemoryTrackerPtr create(Int64 limit = 0)
    {
        if (limit == 0)
        {
            return std::shared_ptr<MemoryTracker>(new MemoryTracker);
        }
        else
        {
            return std::shared_ptr<MemoryTracker>(new MemoryTracker(limit));
        }
    }

    ~MemoryTracker();

    /** Call the following functions before calling of corresponding operations with memory allocators.
      */
    void alloc(Int64 size, bool check_memory_limit = true);

    void realloc(Int64 old_size, Int64 new_size) { alloc(new_size - old_size); }

    /** This function should be called after memory deallocation.
      */
    void free(Int64 size);

    Int64 get() const { return amount.load(std::memory_order_relaxed); }

    Int64 getPeak() const { return peak.load(std::memory_order_relaxed); }

    Int64 getLimit() const { return limit.load(std::memory_order_relaxed); }

    void setLimit(Int64 limit_) { limit.store(limit_, std::memory_order_relaxed); }

    /** Set limit if it was not set.
      * Otherwise, set limit to new value, if new value is greater than previous limit.
      */
    void setOrRaiseLimit(Int64 value);

    void setBytesThatRssLargerThanLimit(Int64 value) { bytes_rss_larger_than_limit = value; }

    void setFaultProbability(double value) { fault_probability = value; }

    void setAccuracyDiffForTest(double value) { accuracy_diff_for_test = value; }

    /// next should be changed only once: from nullptr to some value.
    void setNext(MemoryTracker * elem)
    {
        next.store(elem, std::memory_order_relaxed);
        next_holder = elem ? elem->shared_from_this() : nullptr;
    }

    /// The memory consumption could be shown in realtime via CurrentMetrics counter
    void setMetric(CurrentMetrics::Metric metric_) { metric = metric_; }

    void setDescription(const char * description_) { description = description_; }

    /// Reset the accumulated data.
    void reset();

    /// Prints info about peak memory consumption into log.
    void logPeakMemoryUsage() const;
};

/** The MemoryTracker object is quite difficult to pass to all places where significant amounts of memory are allocated.
  * Therefore, a thread-local pointer to used MemoryTracker is set, or nullptr if MemoryTracker does not need to be used.
  * This pointer is set when memory consumption is monitored in current thread.
  * So, you just need to pass it to all the threads that handle one request.
  */
#if __APPLE__ && __clang__
extern __thread MemoryTracker * current_memory_tracker;
#else
extern thread_local MemoryTracker * current_memory_tracker;
#endif

/// Convenience methods, that use current_memory_tracker if it is available.
namespace CurrentMemoryTracker
{
void disableThreshold();
void submitLocalDeltaMemory();
Int64 getLocalDeltaMemory();
void alloc(Int64 size);
void realloc(Int64 old_size, Int64 new_size);
void free(Int64 size);
} // namespace CurrentMemoryTracker


#include <boost/noncopyable.hpp>

struct TemporarilyDisableMemoryTracker : private boost::noncopyable
{
    MemoryTracker * memory_tracker;

    TemporarilyDisableMemoryTracker()
    {
        memory_tracker = current_memory_tracker;
        current_memory_tracker = nullptr;
    }

    ~TemporarilyDisableMemoryTracker() { current_memory_tracker = memory_tracker; }
};

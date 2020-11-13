#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include <Storages/Transaction/Types.h>

namespace DB
{

// RateLimiter is to control write rate of background tasks of StorageDeltaMerge
// constructor parameters:
// `balance_increase_rate_`: the increase rate of available balance per second, 0 means no limit
// `alloc_balance_soft_limit_` and `alloc_balance_hard_limit_`: usage explained in the comments of `request()` method
//
// `request()` is the main interface used by clients.
// It receives the task size as the parameter,
// and return the remaining task size after allocate balance to it.
// If the remaining task size is 0, then this task can be processed by clients.
// Otherwise, this task must be hang up and request balance later.
class RateLimiter
{
public:
    RateLimiter(Int64 balance_increase_rate_, Int64 alloc_balance_soft_limit_, Int64 alloc_balance_hard_limit_)
        : balance_increase_rate{balance_increase_rate_},
          alloc_balance_soft_limit{alloc_balance_soft_limit_},
          alloc_balance_hard_limit{alloc_balance_hard_limit_},
          available_bytes{alloc_balance_hard_limit_},
          prev_refilled_time{Clock::now()},
          prev_alloc_time{Clock::now()},
          prev_alloc_balance{0}
    {}

    // clients try to request balance through this method
    // it return `bytes` - <allocated_bytes>
    // process to calculate <allocated_bytes>:
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * balance_increase_rate >= alloc_balance_soft_limit,
    //      then <allocated_bytes> = 0
    //   2. <allocated_bytes> = min(available_bytes, `bytes`)
    size_t request(Int64 bytes);

private:
    void refillIfNeed();

private:
    Int64 balance_increase_rate;

    Int64 alloc_balance_soft_limit;
    Int64 alloc_balance_hard_limit;
    Int64 available_bytes;

    Timepoint prev_refilled_time;

    Timepoint prev_alloc_time;
    Int64 prev_alloc_balance;

    std::mutex mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

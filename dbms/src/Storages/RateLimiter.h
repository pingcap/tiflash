#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

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
    RateLimiter(Context & db_context, Int64 balance_increase_rate_, Int64 alloc_balance_soft_limit_, Int64 alloc_balance_hard_limit_);

    // Clients try to request balance through this method,
    // and its' return value means `bytes` - <allocated_bytes>.
    //
    // the current available balance is kept in member `available_bytes`,
    // and `available_bytes` is always less than or equal `alloc_balance_hard_limit`
    //
    // process to calculate <allocated_bytes>:
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * balance_increase_rate >= alloc_balance_soft_limit,
    //      then <allocated_bytes> = 0
    //   2. else, <allocated_bytes> = min(`available_bytes`, `bytes`)
    size_t request(Int64 bytes);

private:
    void refillIfNeed();

private:
    Context & context;

    Int64 balance_increase_rate;

    Int64 alloc_balance_soft_limit;
    Int64 alloc_balance_hard_limit;
    Int64 available_bytes;

    AtomicStopwatch refill_stop_watch;

    AtomicStopwatch alloc_stop_watch;
    Int64 prev_alloc_balance;

    Logger * log;

    std::mutex mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

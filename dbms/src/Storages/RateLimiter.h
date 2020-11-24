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
// `rate_limit_`: the increase rate of available balance per second, 0 means no limit
// `burst_rate_limit_` and `max_balance_`: usage explained in the comments of `request()` method
//
// `request()` is the main interface used by clients.
// It receives the task size as the parameter,
// and return the remaining task size after allocate balance to it.
// If the remaining task size is 0, then this task can be processed by clients.
// Otherwise, this task must be hang up and request balance later.
class RateLimiter
{
public:
    RateLimiter(Context & db_context, Int64 rate_limit_, Int64 burst_rate_limit_, Int64 max_balance_);

    // Clients try to request balance through this method,
    // and its' return value means `bytes` - <allocated_bytes>.
    //
    // the current available balance is kept in member `available_bytes`,
    // and `available_bytes` is always less than or equal `max_balance`
    //
    // process to calculate <allocated_bytes>:
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * rate_limit >= burst_rate_limit,
    //      then <allocated_bytes> = 0
    //   2. else, <allocated_bytes> = min(`available_bytes`, `bytes`)
    size_t request(Int64 bytes);

private:
    void refillIfNeed();

private:
    Context & context;

    Int64 rate_limit;

    Int64 burst_rate_limit;
    Int64 max_balance;
    Int64 available_bytes;

    AtomicStopwatch refill_stop_watch;

    AtomicStopwatch alloc_stop_watch;
    Int64 prev_alloc_balance;

    Logger * log;

    std::mutex mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

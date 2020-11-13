#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include <Storages/Transaction/Types.h>

namespace DB
{

// RateLimiter is to control write rate of background tasks of StorageDeltaMerge
// `max_rate_bytes_balance` TODO: meaning
//
// `request()` is the main interface used by clients.
// It receives the task size as the parameter,
// and return the remaining task size after allocate balance to it.
// If the remaining task size is 0, then this task can be processed by clients.
// Otherwise, this task must be hang up and request balance later.
class RateLimiter
{
public:
    explicit RateLimiter(Int64 max_rate_bytes_balance_)
        : max_rate_bytes_balance{max_rate_bytes_balance_}, available_bytes{max_rate_bytes_balance}, prev_refilled_time{Clock::now()}
    {}

    size_t request(Int64 bytes);

private:
    void refillIfNeed();

private:
    // refill every 500ms
    const size_t refill_period_us = 500 * 1000;
    // 10s refill will make available_bytes increase from 0 to `max_rate_bytes_balance`
    const size_t max_refill_period_count = 10 * 1000 * 1000 / refill_period_us;

    Int64 max_rate_bytes_balance;

    Int64 available_bytes;

    Timepoint prev_refilled_time;

    std::mutex mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

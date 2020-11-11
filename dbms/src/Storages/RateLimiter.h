#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include <Storages/Transaction/Types.h>

namespace DB
{

class RateLimiter
{
public:
    explicit RateLimiter(Int64 rate_bytes_per_sec_)
        : rate_bytes_per_sec{rate_bytes_per_sec_}, available_bytes{rate_bytes_per_sec_}, refilled_time{Clock::now()}
    {}

    size_t request(Int64 bytes);

private:
    void refillIfNeed();

private:
    const size_t refill_period_us = 100 * 1000;

    Int64 rate_bytes_per_sec;

    Int64 available_bytes;

    Timepoint refilled_time;

    std::mutex mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include <Common/Stopwatch.h>

// TODO: separate IO utility(i.e. FileProvider, RateLimiter) from Encryption directory
namespace DB
{

class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

// RateLimiter is to control write rate of background tasks
// constructor parameters:
// `rate_limit_per_sec_`: controls the total write rate of background tasks in bytes per second, 0 means no limit
// `refill_period_us`: this controls how often balance are refilled.
//   For example, when rate_limit_per_sec_ is set to 10MB/s and refill_period_us is set to 100ms,
//   then 1MB is refilled every 100ms internally.
//   Larger value can lead to burstier writes while smaller value introduces more CPU overhead.
//   The default should work for most cases.
class RateLimiter
{
public:
    RateLimiter(TiFlashMetricsPtr metrics_, UInt64 rate_limit_per_sec_, UInt64 refill_period_ms_ = 100);

    ~RateLimiter();

    // `request()` is the main interface used by clients.
    // It receives the requested balance as the parameter,
    // and blocks until the request balance is satisfied.
    void request(UInt64 bytes);

    // just for test purpose
    inline UInt64 getTotalBytesThrough() const { return total_bytes_through; }

private:
    void refillAndAlloc();

    inline UInt64 calculateRefillBalancePerPeriod(UInt64 rate_limit_per_sec_) const
    {
        auto refill_period_per_second = std::max(1, 1000 / refill_period_ms);
        return rate_limit_per_sec_ / refill_period_per_second;
    }

private:
    // used to represent pending request
    struct Request
    {
        explicit Request(UInt64 bytes) : remaining_bytes(bytes), bytes(bytes), granted(false) {}
        UInt64 remaining_bytes;
        UInt64 bytes;
        std::condition_variable cv;
        bool granted;
    };

private:
    UInt64 refill_period_ms;
    AtomicStopwatch refill_stop_watch;

    UInt64 refill_balance_per_period;
    UInt64 available_balance;

    UInt64 total_bytes_through;

    bool stop;
    std::condition_variable exit_cv;
    UInt32 requests_to_wait;

    using RequestQueue = std::deque<Request *>;
    RequestQueue req_queue;

    TiFlashMetricsPtr metrics;
    std::mutex request_mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

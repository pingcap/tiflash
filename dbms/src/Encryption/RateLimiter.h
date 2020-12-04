#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include <Common/Stopwatch.h>

// TODO: separate IO utility(i.e. FileProvider, RateLimiter) from Encryption directory
namespace DB
{

// TODO: add comment to explain the whole class
class RateLimiter
{
public:
    RateLimiter(UInt64 rate_limit_per_sec_, UInt64 refill_period_ms_ = 100);

    ~RateLimiter();

    // TODO: add comment to explain this method
    void request(UInt64 bytes);

private:
    void refillAndAlloc();

    inline UInt64 calculateRefillBalancePerPeriod(UInt64 rate_limit_per_sec_) const
    {
        auto refill_period_per_second = std::max(1, 1000 / refill_period_ms);
        return rate_limit_per_sec_ / refill_period_per_second;
    }

private:
    // Pending request
    struct Request
    {
        explicit Request(UInt64 bytes) : request_bytes(bytes), bytes(bytes), granted(false) {}
        UInt64 request_bytes;
        UInt64 bytes;
        std::condition_variable cv;
        bool granted;
    };

private:
    UInt64 refill_period_ms;
    AtomicStopwatch refill_stop_watch;

    UInt64 refill_balance_per_period;
    UInt64 available_balance;

    bool stop;
    std::condition_variable exit_cv;

    using RequestQueue = std::deque<Request *>;
    RequestQueue req_queue;

    std::mutex request_mutex;
};

using RateLimiterPtr = std::shared_ptr<RateLimiter>;

} // namespace DB

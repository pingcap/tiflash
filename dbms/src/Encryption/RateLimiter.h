#pragma once

#include <Common/Stopwatch.h>
#include <Server/StorageConfigParser.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

// TODO: separate IO utility(i.e. FileProvider, RateLimiter) from Encryption directory
namespace Poco::Util
{
class AbstractConfiguration;
}
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
    RateLimiter(TiFlashMetricsPtr metrics_, Int64 rate_limit_per_sec_, UInt64 refill_period_ms_ = 100);

    virtual ~RateLimiter();

    // `request()` is the main interface used by clients.
    // It receives the requested balance as the parameter,
    // and blocks until the request balance is satisfied.
    void request(Int64 bytes);

    // just for test purpose
    inline UInt64 getTotalBytesThrough() const { return total_bytes_through; }

protected:
    virtual bool canGrant(Int64 bytes);
    virtual void consumeBytes(Int64 bytes);
    virtual void refillAndAlloc();

    inline Int64 calculateRefillBalancePerPeriod(Int64 rate_limit_per_sec_) const
    {
        auto refill_period_per_second = std::max(1, 1000 / refill_period_ms);
        return rate_limit_per_sec_ / refill_period_per_second;
    }

    // used to represent pending request
    struct Request
    {
        explicit Request(Int64 bytes) : remaining_bytes(bytes), bytes(bytes), granted(false) {}
        Int64 remaining_bytes;
        Int64 bytes;
        std::condition_variable cv;
        bool granted;
    };

    UInt64 refill_period_ms;
    AtomicStopwatch refill_stop_watch;

    Int64 refill_balance_per_period;
    Int64 available_balance;

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

class ReadLimiter final : public RateLimiter
{
public:
    ReadLimiter(std::function<Int64()> getIOStatistic_, TiFlashMetricsPtr metrics_, Int64 rate_limit_per_sec_, UInt64 refill_period_ms_ = 100);

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif

    virtual void refillAndAlloc() override;
    virtual void consumeBytes(Int64 bytes) override;
    virtual bool canGrant(Int64 bytes) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Int64 getAvailableBalance();
    Int64 refreshAvailableBalance();

    std::function<Int64()> getIOStatistic;
    Int64 last_stat_bytes;

    using TimePoint = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>;
    TimePoint last_stat_time;

    static constexpr UInt64 get_io_statistic_period_us = 2000;
};

class IORateLimiter
{
public:
    IORateLimiter() = default;

    RateLimiterPtr getWriteLimiter();

    void updateConfig(TiFlashMetricsPtr metrics_, Poco::Util::AbstractConfiguration & config_, Poco::Logger * log_);
    
    void setBackgroundThreadIds(std::vector<pid_t> thread_ids); 

    struct IOInfo
    {
        Int64 total_write_bytes;
        Int64 total_read_bytes;
        Int64 bg_write_bytes;
        Int64 bg_read_bytes;
        std::chrono::time_point<std::chrono::system_clock> uptime_time;

        IOInfo() : total_write_bytes(0), total_read_bytes(0), bg_write_bytes(0), bg_read_bytes(0) {}

        std::string toString() const
        {
            return "total_write_bytes: " + std::to_string(total_write_bytes) +
                   " total_read_bytes: " + std::to_string(total_read_bytes) +
                   " bg_write_bytes: " + std::to_string(bg_write_bytes) + 
                   " bg_read_bytes: " +  std::to_string(bg_read_bytes);
        }
    };

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    std::pair<Int64, Int64> getReadWriteBytes(const std::string& fname, Poco::Logger* log_);
    IOInfo getCurrentIOInfo(Poco::Logger* log_);

    StorageIORateLimitConfig io_config;
    RateLimiterPtr bg_write_limiter;
    RateLimiterPtr fg_write_limiter;
    RateLimiterPtr bg_read_limiter;
    RateLimiterPtr fg_read_limiter;
    std::mutex mtx_;

    std::vector<pid_t> bg_thread_ids;
    IOInfo last_io_info;

    // Noncopyable and nonmovable.
    IORateLimiter(const IORateLimiter & limiter) = delete;
    IORateLimiter & operator=(const IORateLimiter & limiter) = delete;
    IORateLimiter(IORateLimiter && limiter) = delete;
    IORateLimiter && operator=(IORateLimiter && limiter) = delete;
};
} // namespace DB

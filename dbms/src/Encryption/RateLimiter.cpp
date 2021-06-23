#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

#include <cassert>
#include <fstream>

#include <boost/algorithm/string.hpp>

namespace CurrentMetrics
{
extern const Metric RateLimiterPendingWriteRequest;
}
namespace DB
{
RateLimiter::RateLimiter(TiFlashMetricsPtr metrics_, UInt64 rate_limit_per_sec_, UInt64 refill_period_ms_)
    : refill_period_ms{refill_period_ms_},
      refill_balance_per_period{calculateRefillBalancePerPeriod(rate_limit_per_sec_)},
      available_balance{refill_balance_per_period},
      total_bytes_through{0},
      stop{false},
      metrics{std::move(metrics_)}
{}

RateLimiter::~RateLimiter()
{
    std::unique_lock<std::mutex> lock(request_mutex);
    stop = true;
    requests_to_wait = req_queue.size();
    for (auto * r : req_queue)
        r->cv.notify_one();
    while (requests_to_wait > 0)
        exit_cv.wait(lock);
}

void RateLimiter::request(UInt64 bytes)
{
    std::unique_lock<std::mutex> lock(request_mutex);

    if (stop)
        return;

    // 0 means no limit
    if (!refill_balance_per_period)
        return;

    if (metrics)
        GET_METRIC(metrics, tiflash_storage_rate_limiter_total_request_bytes).Increment(bytes);

    if (available_balance >= bytes)
    {
        if (metrics)
            GET_METRIC(metrics, tiflash_storage_rate_limiter_total_alloc_bytes).Increment(bytes);
        total_bytes_through += bytes;
        available_balance -= bytes;
        return;
    }

    CurrentMetrics::Increment pending_request{CurrentMetrics::RateLimiterPendingWriteRequest};

    // request cannot be satisfied at this moment, enqueue
    Request r(bytes);
    req_queue.push_back(&r);
    while (!r.granted)
    {
        assert(!req_queue.empty());

        bool timed_out = false;
        // if this request is in the front of req_queue,
        // then it is responsible to trigger the refill process.
        if (req_queue.front() == &r)
        {
            if (refill_stop_watch.elapsedMilliseconds() >= refill_period_ms)
            {
                timed_out = true;
            }
            else
            {
                auto status = r.cv.wait_for(lock, std::chrono::milliseconds(refill_period_ms));
                timed_out = (status == std::cv_status::timeout);
            }
            if (timed_out)
            {
                refill_stop_watch.restart();
            }
        }
        else
        {
            // Not at the front of queue, just wait
            r.cv.wait(lock);
        }

        // request_mutex is held from now on
        if (stop)
        {
            requests_to_wait--;
            exit_cv.notify_one();
            return;
        }

        // time to do refill
        if (req_queue.front() == &r && timed_out)
        {
            refillAndAlloc();

            if (r.granted)
            {
                // current leader is granted with enough balance,
                // notify the current header of the queue.
                if (!req_queue.empty())
                    req_queue.front()->cv.notify_one();
                break;
            }
        }
    }
}

void RateLimiter::refillAndAlloc()
{
    if (available_balance < refill_balance_per_period)
        available_balance += refill_balance_per_period;

    assert(!req_queue.empty());
    auto * head_req = req_queue.front();
    while (!req_queue.empty())
    {
        auto * next_req = req_queue.front();
        if (available_balance < next_req->remaining_bytes)
        {
            // Decrease remaining_bytes to avoid starvation of this request
            next_req->remaining_bytes -= available_balance;
            total_bytes_through += available_balance;
            available_balance = 0;
            break;
        }
        available_balance -= next_req->remaining_bytes;
        total_bytes_through += next_req->remaining_bytes;
        next_req->remaining_bytes = 0;
        next_req->granted = true;
        req_queue.pop_front();
        if (metrics)
            GET_METRIC(metrics, tiflash_storage_rate_limiter_total_alloc_bytes).Increment(next_req->bytes);
        // quota granted, signal the thread
        if (next_req != head_req)
            next_req->cv.notify_one();
    }
}

#if __APPLE__ && __clang__
extern __thread bool is_background_thread;
#else
extern thread_local bool is_background_thread;
#endif

RateLimiterPtr IORateLimiter::getWriteLimiter()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return is_background_thread ? bg_write_limiter : fg_write_limiter;
}

void IORateLimiter::updateConfig(TiFlashMetricsPtr metrics_, Poco::Util::AbstractConfiguration & config_, Poco::Logger * log_)
{
    StorageIORateLimitConfig new_io_config;
    if (config_.has("storage.io-rate-limit"))
    {
        new_io_config.parse(config_.getString("storage.io-rate-limit"), log_);
    }
    else
    {
        LOG_INFO(log_, "storage.io-rate-limit is not found in config, use default config.");
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (io_config == new_io_config)
    {
        return; // Config is not changes.
    }

    io_config = new_io_config;

    auto GenRateLimiter = [&](UInt64 bytes_per_sec)
    {
        return bytes_per_sec == 0 ? nullptr : std::make_shared<RateLimiter>(metrics_, bytes_per_sec);
    };

    bg_write_limiter = GenRateLimiter(io_config.getBgWriteMaxBytesPerSec());
    fg_write_limiter = GenRateLimiter(io_config.getFgWriteMaxBytesPerSec());
}

bool IORateLimiter::readLimited() const
{
    return is_background_thread ? bg_read_limited.load(std::memory_order_relaxed) : fg_read_limited.load(std::memory_order_relaxed);
}

std::pair<Int64, Int64> IORateLimiter::readTaskIOInfo(const std::string& fname, Poco::Logger* log_)
{
    std::ifstream ifs(fname);
    std::string s;

    Int64 read_bytes = -1;
    Int64 write_bytes = -1;
    while (std::getline(ifs, s))
    {
        std::vector<std::string> values;
        boost::split(values, s, boost::is_any_of(":"));
        if (values.size() != 2)
        {
            LOG_WARNING(log_, "readTaskIOInfo: " << s << " is invalid.");
            continue;
        }
        if (values[0] == "read_bytes")
        {
            boost::algorithm::trim(values[1]);
            read_bytes = std::stoll(values[1]);
        }
        else if (values[0] == "write_bytes")
        {
            boost::algorithm::trim(values[1]);
            write_bytes = std::stoll(values[1]);
        }
    }
    return {read_bytes, write_bytes};
}
    // I/O of foreground thread = I/O of total - I/O of background thread
} // namespace DB

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <boost/algorithm/string.hpp>
#include <cassert>
#include <fstream>

namespace CurrentMetrics
{
extern const Metric RateLimiterPendingWriteRequest;
}
namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

inline void metricRequestBytes(TiFlashMetricsPtr & metrics, LimiterType type, Int64 bytes)
{
    if (unlikely(metrics == nullptr))
    {
        return;
    }
    switch (type)
    {
        case LimiterType::FG_READ:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_fg_read_req_bytes).Increment(bytes);
            break;
        case LimiterType::BG_READ:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_bg_read_req_bytes).Increment(bytes);
            break;
        case LimiterType::FG_WRITE:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_fg_write_req_bytes).Increment(bytes);
            break;
        case LimiterType::BG_WRITE:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_bg_write_req_bytes).Increment(bytes);
            break;
        default:
            break;
    }
}

inline void metricAllocBytes(TiFlashMetricsPtr & metrics, LimiterType type, Int64 bytes)
{
    if (unlikely(metrics == nullptr))
    {
        return;
    }
    switch (type)
    {
        case LimiterType::FG_READ:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_fg_read_alloc_bytes).Increment(bytes);
            break;
        case LimiterType::BG_READ:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_bg_read_alloc_bytes).Increment(bytes);
            break;
        case LimiterType::FG_WRITE:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_fg_write_alloc_bytes).Increment(bytes);
            break;
        case LimiterType::BG_WRITE:
            GET_METRIC(metrics, tiflash_storage_io_limiter, type_bg_write_alloc_bytes).Increment(bytes);
            break;
        default:
            break;
    }
}

WriteLimiter::WriteLimiter(TiFlashMetricsPtr metrics_, Int64 rate_limit_per_sec_, LimiterType type_, UInt64 refill_period_ms_)
    : refill_period_ms{refill_period_ms_},
      refill_balance_per_period{calculateRefillBalancePerPeriod(rate_limit_per_sec_)},
      available_balance{refill_balance_per_period},
      total_bytes_through{0},
      stop{false},
      requests_to_wait{0},
      metrics{std::move(metrics_)},
      type(type_)
{}

WriteLimiter::~WriteLimiter()
{
    setStop();
}

void WriteLimiter::request(Int64 bytes)
{
    std::unique_lock<std::mutex> lock(request_mutex);

    if (stop)
        return;

    // 0 means no limit
    if (!refill_balance_per_period)
        return;

    metricRequestBytes(metrics, type, bytes);
    if (canGrant(bytes))
    {
        consumeBytes(bytes);
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
            UInt64 elapsed_ms = refill_stop_watch.elapsedMilliseconds();
            if (elapsed_ms >= refill_period_ms)
            {
                timed_out = true;
            }
            else
            {
                // Wait for next refill period.
                auto status = r.cv.wait_for(lock, std::chrono::milliseconds(refill_period_ms - elapsed_ms));
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

size_t WriteLimiter::setStop()
{
    std::unique_lock lock(request_mutex);
    if (stop) // Already set stopped.
    {
        return 0;
    }

    stop = true;
    // Notify all waitting threads.
    requests_to_wait = req_queue.size();
    auto sz = requests_to_wait;
    for (auto * r : req_queue)
    {
        r->cv.notify_one();
    }
    // Wait threads wakeup and return.
    while (requests_to_wait > 0)
    {
        exit_cv.wait(lock);
    }
    return sz;
}

bool WriteLimiter::canGrant(Int64 bytes) { return available_balance >= bytes; }

void WriteLimiter::consumeBytes(Int64 bytes)
{
    metricAllocBytes(metrics, type, bytes);
    total_bytes_through += bytes;
    available_balance -= bytes;
}

void WriteLimiter::refillAndAlloc()
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
            metricAllocBytes(metrics, type, available_balance);
            available_balance = 0;
            break;
        }
        consumeBytes(next_req->remaining_bytes);
        next_req->remaining_bytes = 0;
        next_req->granted = true;
        req_queue.pop_front();
        // quota granted, signal the thread
        if (next_req != head_req)
            next_req->cv.notify_one();
    }
}

ReadLimiter::ReadLimiter(std::function<Int64()> getIOStatistic_, TiFlashMetricsPtr metrics_, Int64 rate_limit_per_sec_, LimiterType type_,
    Int64 get_io_stat_period_us, UInt64 refill_period_ms_)
    : WriteLimiter(metrics_, rate_limit_per_sec_, type_, refill_period_ms_),
      getIOStatistic(std::move(getIOStatistic_)),
      last_stat_bytes(getIOStatistic()),
      last_stat_time(now()),
      log(&Poco::Logger::get("ReadLimiter")),
      get_io_statistic_period_us(get_io_stat_period_us)
{}

Int64 ReadLimiter::getAvailableBalance()
{
    TimePoint us = now();
    // Not call getIOStatisctics() every time for performance.
    // If the clock back, elapsed_us could be negative.
    Int64 elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(us - last_stat_time).count();
    if (get_io_statistic_period_us != 0 && elapsed_us < get_io_statistic_period_us)
    {
        return available_balance;
    }

    return refreshAvailableBalance();
}

Int64 ReadLimiter::refreshAvailableBalance()
{
    TimePoint us = now();
    Int64 bytes = getIOStatistic();
    if (bytes < last_stat_bytes)
    {
        LOG_WARNING(log,
            fmt::format("last_stat {}:{} current_stat {}:{}", last_stat_time.time_since_epoch().count(), last_stat_bytes,
                us.time_since_epoch().count(), bytes));
    }
    else
    {
        Int64 real_alloc_bytes = bytes - last_stat_bytes;
        metricAllocBytes(metrics, type, real_alloc_bytes);
        available_balance -= real_alloc_bytes;
    }
    last_stat_bytes = bytes;
    last_stat_time = us;
    return available_balance;
}

void ReadLimiter::consumeBytes(Int64 bytes)
{
    metricRequestBytes(metrics, type, bytes);
    // Do nothing for read.
}

bool ReadLimiter::canGrant([[maybe_unused]] Int64 bytes)
{
    return getAvailableBalance() > 0;
}

void ReadLimiter::refillAndAlloc()
{
    if (available_balance < refill_balance_per_period)
    {
        available_balance += refill_balance_per_period;
    }

    assert(!req_queue.empty());
    auto * head_req = req_queue.front();
    while (!req_queue.empty())
    {
        auto * next_req = req_queue.front();
        if (!canGrant(next_req->remaining_bytes))
        {
            break;
        }

        next_req->remaining_bytes = 0;
        next_req->granted = true;

        req_queue.pop_front();
        if (next_req != head_req)
        {
            next_req->cv.notify_one();
        }
    }
}

IORateLimiter::IORateLimiter() : log(&Poco::Logger::get("IORateLimiter")) {}

#if __APPLE__ && __clang__
extern __thread bool is_background_thread;
#else
extern thread_local bool is_background_thread;
#endif

WriteLimiterPtr IORateLimiter::getWriteLimiter()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return is_background_thread ? bg_write_limiter : fg_write_limiter;
}

ReadLimiterPtr IORateLimiter::getReadLimiter()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return is_background_thread ? bg_read_limiter : fg_read_limiter;
}

void IORateLimiter::updateConfig(TiFlashMetricsPtr metrics_, Poco::Util::AbstractConfiguration & config_)
{
    StorageIORateLimitConfig new_io_config;
    if (config_.has("storage.io_rate_limit"))
    {
        new_io_config.parse(config_.getString("storage.io_rate_limit"), log);
    }
    else
    {
        LOG_INFO(log, "storage.io_rate_limit is not found in config, use default config.");
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (io_config == new_io_config)
    {
        LOG_INFO(log, "storage.io_rate_limit is not changed");
        return;
    }

    LOG_INFO(log, "storage.io_rate_limit is changed, update limiter.");
    io_config = new_io_config;

    auto GenRateLimiter = [&](UInt64 bytes_per_sec, LimiterType type) {
        return bytes_per_sec == 0 ? nullptr : std::make_shared<WriteLimiter>(metrics_, bytes_per_sec, type);
    };

    bg_write_limiter = GenRateLimiter(io_config.getBgWriteMaxBytesPerSec(), LimiterType::BG_WRITE);
    fg_write_limiter = GenRateLimiter(io_config.getFgWriteMaxBytesPerSec(), LimiterType::FG_WRITE);
#ifdef __linux__
    {
        auto bytes = io_config.getBgReadMaxBytesPerSec();
        auto getBgReadIOStatistic = [&]() { return getCurrentIOInfo().bg_read_bytes; };
        bg_read_limiter = bytes == 0 ? nullptr : std::make_shared<ReadLimiter>(getBgReadIOStatistic, metrics_, bytes, LimiterType::BG_READ);
    }

    {
        auto bytes = io_config.getFgReadMaxBytesPerSec();
        auto getFgReadIOStatistic = [&]() {
            auto io_info = getCurrentIOInfo();
            return std::max(0, io_info.total_read_bytes - io_info.bg_read_bytes);
        };
        fg_read_limiter = bytes == 0 ? nullptr : std::make_shared<ReadLimiter>(getFgReadIOStatistic, metrics_, bytes, LimiterType::FG_READ);
    }
#endif
}

void IORateLimiter::setBackgroundThreadIds(std::vector<pid_t> thread_ids)
{
    std::lock_guard lock(bg_thread_ids_mtx);
    bg_thread_ids.swap(thread_ids);
}

std::pair<Int64, Int64> IORateLimiter::getReadWriteBytes(const std::string & fname)
{
#if __linux__
    std::ifstream ifs(fname);
    if (ifs.fail())
    {
        auto msg = fmt::format("open {} fail: {}", fname, strerror(errno));
        LOG_ERROR(log, msg);
        throw Exception(msg, ErrorCodes::UNKNOWN_EXCEPTION);
    }
    std::string s;
    Int64 read_bytes = -1;
    Int64 write_bytes = -1;
    while (std::getline(ifs, s))
    {
        if (s.empty())
        {
            continue;
        }
        std::vector<std::string> values;
        boost::split(values, s, boost::is_any_of(":"));
        if (values.size() != 2)
        {
            LOG_WARNING(log, "readTaskIOInfo: " << s << " is invalid.");
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
    if (read_bytes == -1 || write_bytes == -1)
    {
        auto msg = fmt::format("read_bytes: {} write_bytes: {} Invalid result.", read_bytes, write_bytes);
        LOG_ERROR(log, msg);
        throw Exception(msg, ErrorCodes::UNKNOWN_EXCEPTION);
    }
    return {read_bytes, write_bytes};
#else
    return {0, 0};
#endif
}

IORateLimiter::IOInfo IORateLimiter::getCurrentIOInfo()
{
    static const pid_t pid = getpid();
    IOInfo io_info;

    // Read I/O info of each background threads.
    for (pid_t tid : bg_thread_ids)
    {
        const std::string thread_io_fname = fmt::format("/proc/{}/task/{}/io", pid, tid);
        Int64 read_bytes, write_bytes;
        std::tie(read_bytes, write_bytes) = getReadWriteBytes(thread_io_fname);
        io_info.bg_read_bytes += read_bytes;
        io_info.bg_write_bytes += write_bytes;
    }

    // Read I/O info of this process.
    static const std::string proc_io_fname = fmt::format("/proc/{}/io", pid);
    std::tie(io_info.total_read_bytes, io_info.total_write_bytes) = getReadWriteBytes(proc_io_fname);
    io_info.update_time = std::chrono::system_clock::now();
    return io_info;
}

void IORateLimiter::setStop()
{
    std::lock_guard lock(mtx_);
    if (bg_write_limiter != nullptr)
    {
        auto sz = bg_write_limiter->setStop();
        LOG_DEBUG(log, "bg_write_limiter setStop request size " << sz);
    }
    if (fg_write_limiter != nullptr)
    {
        auto sz = fg_write_limiter->setStop();
        LOG_DEBUG(log, "fg_write_limiter setStop request size " << sz);
    }
    if (bg_read_limiter != nullptr)
    {
        auto sz = bg_read_limiter->setStop();
        LOG_DEBUG(log, "bg_read_limiter setStop request size " << sz);
    }
    if (fg_read_limiter != nullptr)
    {
        auto sz = fg_read_limiter->setStop();
        LOG_DEBUG(log, "fg_read_limiter setStop request size " << sz);
    }
}
} // namespace DB

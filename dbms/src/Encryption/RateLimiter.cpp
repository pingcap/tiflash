#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/RateLimiter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string.hpp>
#include <cassert>
#include <fstream>

namespace CurrentMetrics
{
extern const Metric RateLimiterPendingWriteRequest;
extern const Metric IOLimiterPendingFgReadReq;
extern const Metric IOLimiterPendingBgReadReq;
extern const Metric IOLimiterPendingFgWriteReq;
extern const Metric IOLimiterPendingBgWriteReq;
} // namespace CurrentMetrics
namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

inline void metricRequestBytes(LimiterType type, Int64 bytes)
{
    switch (type)
    {
    case LimiterType::FG_READ:
        GET_METRIC(tiflash_storage_io_limiter, type_fg_read_req_bytes).Increment(bytes);
        break;
    case LimiterType::BG_READ:
        GET_METRIC(tiflash_storage_io_limiter, type_bg_read_req_bytes).Increment(bytes);
        break;
    case LimiterType::FG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter, type_fg_write_req_bytes).Increment(bytes);
        break;
    case LimiterType::BG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter, type_bg_write_req_bytes).Increment(bytes);
        break;
    default:
        break;
    }
}

inline void metricAllocBytes(LimiterType type, Int64 bytes)
{
    switch (type)
    {
    case LimiterType::FG_READ:
        GET_METRIC(tiflash_storage_io_limiter, type_fg_read_alloc_bytes).Increment(bytes);
        break;
    case LimiterType::BG_READ:
        GET_METRIC(tiflash_storage_io_limiter, type_bg_read_alloc_bytes).Increment(bytes);
        break;
    case LimiterType::FG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter, type_fg_write_alloc_bytes).Increment(bytes);
        break;
    case LimiterType::BG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter, type_bg_write_alloc_bytes).Increment(bytes);
        break;
    default:
        break;
    }
}

inline CurrentMetrics::Increment pendingRequestMetrics(LimiterType type)
{
    switch (type)
    {
    case LimiterType::FG_READ:
        return CurrentMetrics::Increment{CurrentMetrics::IOLimiterPendingFgReadReq};
    case LimiterType::BG_READ:
        return CurrentMetrics::Increment{CurrentMetrics::IOLimiterPendingBgReadReq};
    case LimiterType::FG_WRITE:
        return CurrentMetrics::Increment{CurrentMetrics::IOLimiterPendingFgWriteReq};
    case LimiterType::BG_WRITE:
        return CurrentMetrics::Increment{CurrentMetrics::IOLimiterPendingBgWriteReq};
    default:
        return CurrentMetrics::Increment{CurrentMetrics::RateLimiterPendingWriteRequest};
    }
}

WriteLimiter::WriteLimiter(Int64 rate_limit_per_sec_, LimiterType type_, UInt64 refill_period_ms_)
    : refill_period_ms{refill_period_ms_}
    , refill_balance_per_period{calculateRefillBalancePerPeriod(rate_limit_per_sec_)}
    , available_balance{refill_balance_per_period}
    , stop{false}
    , requests_to_wait{0}
    , type(type_)
    , alloc_bytes{0}
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

    metricRequestBytes(type, bytes);
    if (canGrant(bytes))
    {
        consumeBytes(bytes);
        return;
    }

    auto pending_request = pendingRequestMetrics(type);

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

bool WriteLimiter::canGrant(Int64 bytes)
{
    return available_balance >= bytes;
}

void WriteLimiter::consumeBytes(Int64 bytes)
{
    metricAllocBytes(type, bytes);
    available_balance -= bytes;
    alloc_bytes += bytes;
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
            metricAllocBytes(type, available_balance);
            alloc_bytes += available_balance;
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

LimiterStat WriteLimiter::getStat()
{
    std::lock_guard lock(request_mutex);
    UInt64 elapsed_ms = stat_stop_watch.elapsedMilliseconds();
    if (refill_period_ms == 0 || refill_balance_per_period == 0 || elapsed_ms < refill_period_ms)
    {
        throw DB::Exception(fmt::format("elapsed_ms {} refill_period_ms {} refill_balance_per_period {} is invalid.",
                                        elapsed_ms,
                                        refill_period_ms,
                                        refill_balance_per_period),
                            ErrorCodes::LOGICAL_ERROR);
    }
    // Get and Reset
    LimiterStat stat(alloc_bytes, elapsed_ms, refill_period_ms, refill_balance_per_period);
    alloc_bytes = 0;
    stat_stop_watch.restart();
    return stat;
}

void WriteLimiter::updateMaxBytesPerSec(Int64 max_bytes_per_sec)
{
    if (max_bytes_per_sec < 0)
    {
        auto msg = fmt::format("updateMaxBytesPerSec: max_bytes_per_sec {} is invalid.", max_bytes_per_sec);
        throw DB::Exception(msg, ErrorCodes::LOGICAL_ERROR);
    }
    std::lock_guard lock(request_mutex);
    refill_balance_per_period = calculateRefillBalancePerPeriod(max_bytes_per_sec);
}

ReadLimiter::ReadLimiter(
    std::function<Int64()> getIOStatistic_,
    Int64 rate_limit_per_sec_,
    LimiterType type_,
    Int64 get_io_stat_period_us,
    UInt64 refill_period_ms_)
    : WriteLimiter(rate_limit_per_sec_, type_, refill_period_ms_)
    , getIOStatistic(std::move(getIOStatistic_))
    , last_stat_bytes(getIOStatistic())
    , last_stat_time(now())
    , log(&Poco::Logger::get("ReadLimiter"))
    , get_io_statistic_period_us(get_io_stat_period_us)
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
                    fmt::format("last_stat {}:{} current_stat {}:{}",
                                last_stat_time.time_since_epoch().count(),
                                last_stat_bytes,
                                us.time_since_epoch().count(),
                                bytes));
    }
    else
    {
        Int64 real_alloc_bytes = bytes - last_stat_bytes;
        metricAllocBytes(type, real_alloc_bytes);
        available_balance -= real_alloc_bytes;
        alloc_bytes += real_alloc_bytes;
    }
    last_stat_bytes = bytes;
    last_stat_time = us;
    return available_balance;
}

void ReadLimiter::consumeBytes(Int64 bytes)
{
    metricRequestBytes(type, bytes);
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
        consumeBytes(next_req->remaining_bytes);
        next_req->remaining_bytes = 0;
        next_req->granted = true;

        req_queue.pop_front();
        if (next_req != head_req)
        {
            next_req->cv.notify_one();
        }
    }
}

IORateLimiter::IORateLimiter()
    : log(&Poco::Logger::get("IORateLimiter"))
    , stop(false)
{}

IORateLimiter::~IORateLimiter()
{
    stop.store(true, std::memory_order_relaxed);
    if (auto_tune_thread.joinable())
    {
        auto_tune_thread.join();
    }
}

void IORateLimiter::init(Poco::Util::AbstractConfiguration & config_)
{
    updateConfig(config_);
    runAutoTune();
}

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

void IORateLimiter::updateConfig(Poco::Util::AbstractConfiguration & config_)
{
    StorageIORateLimitConfig new_io_config;
    if (!readConfig(config_, new_io_config))
    {
        return;
    }
    std::lock_guard lock(mtx_);
    updateReadLimiter(io_config.getBgReadMaxBytesPerSec(), io_config.getFgReadMaxBytesPerSec());
    updateWriteLimiter(io_config.getBgWriteMaxBytesPerSec(), io_config.getFgWriteMaxBytesPerSec());
}

bool IORateLimiter::readConfig(Poco::Util::AbstractConfiguration & config_, StorageIORateLimitConfig & new_io_config)
{
    if (config_.has("storage.io_rate_limit"))
    {
        new_io_config.parse(config_.getString("storage.io_rate_limit"), log);
    }
    else
    {
        LOG_INFO(log, "storage.io_rate_limit is not found in config, use default config.");
    }
    if (io_config == new_io_config)
    {
        LOG_INFO(log, "storage.io_rate_limit is not changed.");
        return false;
    }
    LOG_INFO(log, fmt::format("storage.io_rate_limit is changed: {} => {}", io_config.toString(), new_io_config.toString()));
    io_config = new_io_config;
    return true;
}

void IORateLimiter::updateReadLimiter(Int64 bg_bytes, Int64 fg_bytes)
{
    LOG_INFO(log, fmt::format("updateReadLimiter: bg_bytes {} fg_bytes {}", bg_bytes, fg_bytes));
    auto getBgReadIOStatistic = [&]() {
        return getCurrentIOInfo().bg_read_bytes;
    };
    auto getFgReadIOStatistic = [&]() {
        auto io_info = getCurrentIOInfo();
        return std::max(0, io_info.total_read_bytes - io_info.bg_read_bytes);
    };

    if (bg_bytes == 0)
    {
        bg_read_limiter = nullptr;
    }
    else if (bg_read_limiter == nullptr) // bg_bytes != 0 && bg_read_limiter == nullptr
    {
        bg_read_limiter = std::make_shared<ReadLimiter>(getBgReadIOStatistic, bg_bytes, LimiterType::BG_READ);
    }
    else // bg_bytes != 0 && bg_read_limiter != nullptr
    {
        bg_read_limiter->updateMaxBytesPerSec(bg_bytes);
    }

    if (fg_bytes == 0)
    {
        fg_read_limiter = nullptr;
    }
    else if (fg_read_limiter == nullptr) // fg_bytes != 0 && fg_read_limiter == nullptr
    {
        fg_read_limiter = std::make_shared<ReadLimiter>(getFgReadIOStatistic, fg_bytes, LimiterType::FG_READ);
    }
    else // fg_bytes != 0 && fg_read_limiter != nullptr
    {
        fg_read_limiter->updateMaxBytesPerSec(fg_bytes);
    }
}

void IORateLimiter::updateWriteLimiter(Int64 bg_bytes, Int64 fg_bytes)
{
    LOG_INFO(log, fmt::format("updateWriteLimiter: bg_bytes {} fg_bytes {}", bg_bytes, fg_bytes));
    if (bg_bytes == 0)
    {
        bg_write_limiter = nullptr;
    }
    else if (bg_write_limiter == nullptr)
    {
        bg_write_limiter = std::make_shared<WriteLimiter>(bg_bytes, LimiterType::BG_WRITE);
    }
    else
    {
        bg_write_limiter->updateMaxBytesPerSec(bg_bytes);
    }

    if (fg_bytes == 0)
    {
        fg_write_limiter = nullptr;
    }
    else if (fg_write_limiter == nullptr)
    {
        fg_write_limiter = std::make_shared<WriteLimiter>(fg_bytes, LimiterType::FG_WRITE);
    }
    else
    {
        fg_write_limiter->updateMaxBytesPerSec(fg_bytes);
    }
}

void IORateLimiter::setBackgroundThreadIds(std::vector<pid_t> thread_ids)
{
    std::lock_guard lock(bg_thread_ids_mtx);
    bg_thread_ids.swap(thread_ids);
}

std::pair<Int64, Int64> IORateLimiter::getReadWriteBytes(const std::string & fname [[maybe_unused]])
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

void IORateLimiter::runAutoTune()
{
    auto autoTuneWorker = [&]() {
        while (!stop.load(std::memory_order_relaxed))
        {
            ::sleep(io_config.auto_tune_sec > 0 ? io_config.auto_tune_sec : 1);
            if (io_config.auto_tune_sec > 0)
            {
                autoTune();
            }
        }
    };
    auto_tune_thread = std::thread(autoTuneWorker);
}

std::unique_ptr<IOLimitTuner> IORateLimiter::createIOLimitTuner()
{
    WriteLimiterPtr bg_write, fg_write;
    ReadLimiterPtr bg_read, fg_read;
    StorageIORateLimitConfig io_config_;
    {
        std::lock_guard lock(mtx_);
        bg_write = bg_write_limiter;
        fg_write = fg_write_limiter;
        bg_read = bg_read_limiter;
        fg_read = fg_read_limiter;
        io_config_ = io_config;
    }
    return std::make_unique<IOLimitTuner>(
        bg_write != nullptr ? std::make_unique<LimiterStat>(bg_write->getStat()) : nullptr,
        fg_write != nullptr ? std::make_unique<LimiterStat>(fg_write->getStat()) : nullptr,
        bg_read != nullptr ? std::make_unique<LimiterStat>(bg_read->getStat()) : nullptr,
        fg_read != nullptr ? std::make_unique<LimiterStat>(fg_read->getStat()) : nullptr,
        io_config_);
}

void IORateLimiter::autoTune()
{
    try
    {
        auto tuner = createIOLimitTuner();
        auto tune_result = tuner->tune();
        if (tune_result.read_tuned)
        {
            std::lock_guard lock(mtx_);
            updateReadLimiter(tune_result.max_bg_read_bytes_per_sec, tune_result.max_fg_read_bytes_per_sec);
        }
        if (tune_result.write_tuned)
        {
            std::lock_guard lock(mtx_);
            updateWriteLimiter(tune_result.max_bg_write_bytes_per_sec, tune_result.max_fg_write_bytes_per_sec);
        }
    }
    catch (DB::Exception & e)
    {
        LOG_ERROR(log, e.message());
    }
}


IOLimitTuner::IOLimitTuner(
    LimiterStatUPtr bg_write_stat_,
    LimiterStatUPtr fg_write_stat_,
    LimiterStatUPtr bg_read_stat_,
    LimiterStatUPtr fg_read_stat_,
    const StorageIORateLimitConfig & io_config_)
    : bg_write_stat(std::move(bg_write_stat_))
    , fg_write_stat(std::move(fg_write_stat_))
    , bg_read_stat(std::move(bg_read_stat_))
    , fg_read_stat(std::move(fg_read_stat_))
    , io_config(io_config_)
    , log(&Poco::Logger::get("IOLimitTuner"))
{}

IOLimitTuner::TuneResult IOLimitTuner::tune() const
{
    auto msg = fmt::format("limiter {} write {} read {}", limiterCount(), writeLimiterCount(), readLimiterCount());
    if (limiterCount() < 2)
    {
        LOG_INFO(log, msg << " NOT need to tune.");
        return {0, 0, false, 0, 0, false};
    }
    LOG_INFO(log, msg << " need to tune.");
    if (bg_write_stat)
    {
        LOG_DEBUG(log, "bg_write_stat => " << bg_write_stat->toString());
    }
    if (fg_write_stat)
    {
        LOG_DEBUG(log, "fg_write_stat => " << fg_write_stat->toString());
    }
    if (bg_read_stat)
    {
        LOG_DEBUG(log, "bg_read_stat => " << bg_read_stat->toString());
    }
    if (fg_read_stat)
    {
        LOG_DEBUG(log, "fg_read_stat => " << fg_read_stat->toString());
    }

    auto [max_read_bytes_per_sec, max_write_bytes_per_sec, rw_tuned] = tuneReadWrite();
    LOG_INFO(
        log,
        fmt::format("tuneReadWrite: max_read {} max_write {} rw_tuned {}", max_read_bytes_per_sec, max_write_bytes_per_sec, rw_tuned));
    auto [max_bg_read_bytes_per_sec, max_fg_read_bytes_per_sec, read_tuned] = tuneRead(max_read_bytes_per_sec);
    LOG_INFO(log,
             fmt::format("tuneRead: bg_read {} fg_read {} read_tuned {}", max_bg_read_bytes_per_sec, max_fg_read_bytes_per_sec, read_tuned));
    auto [max_bg_write_bytes_per_sec, max_fg_write_bytes_per_sec, write_tuned] = tuneWrite(max_write_bytes_per_sec);
    LOG_INFO(log,
             fmt::format(
                 "tuneWrite: bg_write {} fg_write {} write_tuned {}",
                 max_bg_write_bytes_per_sec,
                 max_fg_write_bytes_per_sec,
                 write_tuned));
    return {.max_bg_read_bytes_per_sec = max_bg_read_bytes_per_sec,
            .max_fg_read_bytes_per_sec = max_fg_read_bytes_per_sec,
            .read_tuned = read_tuned || rw_tuned,
            .max_bg_write_bytes_per_sec = max_bg_write_bytes_per_sec,
            .max_fg_write_bytes_per_sec = max_fg_write_bytes_per_sec,
            .write_tuned = write_tuned || rw_tuned};
}

// <max_read_bytes_per_sec, max_write_bytes_per_sec, has_tuned>
std::tuple<Int64, Int64, bool> IOLimitTuner::tuneReadWrite() const
{
    // Disk limit of read and write I/O, cannot tune.
    if (!io_config.use_max_bytes_per_sec)
    {
        return {io_config.max_read_bytes_per_sec, io_config.max_write_bytes_per_sec, false};
    }

    // Not limit write and limit read only, not need tune.
    if (writeLimiterCount() == 0)
    {
        assert(readLimiterCount() == 2);
        return {io_config.max_bytes_per_sec, 0, false};
    }

    // Not limit read and limit write only, not need tune.
    if (readLimiterCount() == 0)
    {
        assert(writeLimiterCount() == 2);
        return {0, io_config.max_bytes_per_sec, false};
    }

    TuneInfo read_info(maxReadBytesPerSec(), avgReadBytesPerSec(), readWatermark(), io_config.getReadMaxBytesPerSec());
    TuneInfo write_info(maxWriteBytesPerSec(), avgWriteBytesPerSec(), writeWatermark(), io_config.getWriteMaxBytesPerSec());
    LOG_INFO(log, "read_tune_info => " << read_info.toString() << " write_tune_info => " << write_info.toString());
    return tune(read_info, write_info);
}

// <bg, fg, has_tune>
std::tuple<Int64, Int64, bool> IOLimitTuner::tuneRead(Int64 max_bytes_per_sec) const
{
    return tuneBgFg(
        max_bytes_per_sec,
        bg_read_stat,
        io_config.getBgReadMaxBytesPerSec(),
        fg_read_stat,
        io_config.getFgReadMaxBytesPerSec());
}
std::tuple<Int64, Int64, bool> IOLimitTuner::tuneWrite(Int64 max_bytes_per_sec) const
{
    return tuneBgFg(
        max_bytes_per_sec,
        bg_write_stat,
        io_config.getBgWriteMaxBytesPerSec(),
        fg_write_stat,
        io_config.getFgWriteMaxBytesPerSec());
}

// <bg, fg, has_tune>
std::tuple<Int64, Int64, bool>
IOLimitTuner::tuneBgFg(
    Int64 max_bytes_per_sec,
    const LimiterStatUPtr & bg,
    Int64 config_bg_max_bytes_per_sec,
    const LimiterStatUPtr & fg,
    Int64 config_fg_max_bytes_per_sec) const
{
    if (max_bytes_per_sec == 0)
    {
        return {0, 0, false};
    }
    if (bg == nullptr || fg == nullptr)
    {
        return {bg == nullptr ? 0 : max_bytes_per_sec, fg == nullptr ? 0 : max_bytes_per_sec, false};
    }

    TuneInfo bg_info(bg->maxBytesPerSec(), bg->avgBytesPerSec(), getWatermark(bg->pct()), config_bg_max_bytes_per_sec);
    TuneInfo fg_info(fg->maxBytesPerSec(), fg->avgBytesPerSec(), getWatermark(fg->pct()), config_fg_max_bytes_per_sec);
    LOG_INFO(log, "bg_tune_info => " << bg_info.toString() << " fg_tune_info => " << fg_info.toString());
    auto [tuned_bg_max_bytes_per_sec, tuned_fg_max_bytes_per_sec, has_tuned] = tune(bg_info, fg_info);
    tuned_bg_max_bytes_per_sec = max_bytes_per_sec * tuned_bg_max_bytes_per_sec / (tuned_bg_max_bytes_per_sec + tuned_fg_max_bytes_per_sec);
    tuned_fg_max_bytes_per_sec = max_bytes_per_sec - tuned_bg_max_bytes_per_sec;
    return {tuned_bg_max_bytes_per_sec, tuned_fg_max_bytes_per_sec, has_tuned};
}

bool IOLimitTuner::calculate(Int64 & to_add, Int64 & to_sub, Int64 delta) const
{
    if (to_sub <= io_config.min_bytes_per_sec)
    {
        return false;
    }
    Int64 max_tune_bytes = to_sub - io_config.min_bytes_per_sec;
    Int64 tune_bytes = std::min(delta / io_config.tune_base, max_tune_bytes);
    to_add += tune_bytes;
    to_sub -= tune_bytes;
    return tune_bytes > 0;
}

// <max_bytes_per_sec1, max_bytes_per_sec2, has_tuned>
std::tuple<Int64, Int64, bool> IOLimitTuner::tune(const TuneInfo & t1, const TuneInfo & t2) const
{
    Int64 max_bytes_per_sec1 = t1.max_bytes_per_sec;
    Int64 max_bytes_per_sec2 = t2.max_bytes_per_sec;
    auto watermark1 = t1.watermark;
    auto watermark2 = t2.watermark;
    bool has_tuned = false;

    // Tune emergency.
    if (watermark1 == Watermark::Emergency && watermark2 == Watermark::Emergency)
    {
        if (max_bytes_per_sec1 < t1.config_max_bytes_per_sec)
        {
            has_tuned = calculate(max_bytes_per_sec1, max_bytes_per_sec2, t1.config_max_bytes_per_sec - max_bytes_per_sec1);
        }
        else if (max_bytes_per_sec2 < t2.config_max_bytes_per_sec)
        {
            has_tuned = calculate(max_bytes_per_sec2, max_bytes_per_sec1, t2.config_max_bytes_per_sec - max_bytes_per_sec2);
        }
        return {max_bytes_per_sec1, max_bytes_per_sec2, has_tuned};
    }

    // Both watermark is High/Medium/Low, not need to tune.
    if (watermark1 == watermark2)
    {
        return {max_bytes_per_sec1, max_bytes_per_sec2, false};
    }

    if (watermark1 > watermark2)
    {
        has_tuned = calculate(max_bytes_per_sec1, max_bytes_per_sec2, max_bytes_per_sec2 - t2.avg_bytes_per_sec);
    }
    else
    {
        has_tuned = calculate(max_bytes_per_sec2, max_bytes_per_sec1, max_bytes_per_sec1 - t1.avg_bytes_per_sec);
    }
    return {max_bytes_per_sec1, max_bytes_per_sec2, has_tuned};
}

IOLimitTuner::Watermark IOLimitTuner::getWatermark(int pct) const
{
    if (pct >= io_config.emergency_pct)
    {
        return Watermark::Emergency;
    }
    else if (pct >= io_config.high_pct)
    {
        return Watermark::High;
    }
    else if (pct >= io_config.medium_pct)
    {
        return Watermark::Medium;
    }
    else
    {
        return Watermark::Low;
    }
}
} // namespace DB

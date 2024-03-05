// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/RateLimiter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost_wrapper/string.h>
#include <common/likely.h>
#include <common/logger_useful.h>

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

void metricPendingDuration(LimiterType type, double second)
{
    switch (type)
    {
    case LimiterType::FG_READ:
        GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_fg_read).Observe(second);
        break;
    case LimiterType::BG_READ:
        GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_bg_read).Observe(second);
        break;
    case LimiterType::FG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_fg_write).Observe(second);
        break;
    case LimiterType::BG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_bg_write).Observe(second);
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

void metricPendingCount(LimiterType type)
{
    switch (type)
    {
    case LimiterType::FG_READ:
        GET_METRIC(tiflash_storage_io_limiter_pending_count, type_fg_read).Increment();
        break;
    case LimiterType::BG_READ:
        GET_METRIC(tiflash_storage_io_limiter_pending_count, type_bg_read).Increment();
        break;
    case LimiterType::FG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter_pending_count, type_fg_write).Increment();
        break;
    case LimiterType::BG_WRITE:
        GET_METRIC(tiflash_storage_io_limiter_pending_count, type_bg_write).Increment();
        break;
    default:
        break;
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
    , log(Logger::get(std::string(magic_enum::enum_name(type))))
{}

WriteLimiter::~WriteLimiter()
{
    setStop();
}

void WriteLimiter::request(Int64 bytes)
{
    std::unique_lock lock(request_mutex);

    if (unlikely(stop))
        return;

    // 0 means no limit
    if (unlikely(!refill_balance_per_period))
        return;

    metricRequestBytes(type, bytes);
    if (canGrant(bytes))
    {
        consumeBytes(bytes);
        return;
    }
    Stopwatch sw_pending;
    Int64 wait_times = 0;
    auto pending_request = pendingRequestMetrics(type);
    metricPendingCount(type);
    SCOPE_EXIT({ metricPendingDuration(type, sw_pending.elapsedSeconds()); });
    // request cannot be satisfied at this moment, enqueue
    Request r(bytes);
    req_queue.push_back(&r);
    while (!r.granted)
    {
        assert(!req_queue.empty());
        wait_times++;
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
    LOG_TRACE(
        log,
        "pending_us {} wait_times {} pending_count {} rate_limit_per_sec {}",
        sw_pending.elapsed() / 1000,
        wait_times,
        req_queue.size(),
        refill_balance_per_period * 1000 / refill_period_ms);
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
        throw DB::Exception(
            fmt::format(
                "elapsed_ms {} refill_period_ms {} refill_balance_per_period {} is invalid.",
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
    std::function<Int64()> get_read_bytes_,
    Int64 rate_limit_per_sec_,
    LimiterType type_,
    UInt64 refill_period_ms_)
    : WriteLimiter(rate_limit_per_sec_, type_, refill_period_ms_)
    , get_read_bytes(std::move(get_read_bytes_))
    , last_stat_bytes(get_read_bytes())
    , last_refill_time(std::chrono::system_clock::now())
{}

Int64 ReadLimiter::getAvailableBalance()
{
    Int64 bytes = get_read_bytes();
    if (unlikely(bytes < last_stat_bytes))
    {
        LOG_WARNING(log, "last_stat: {} current_stat: {}", last_stat_bytes, bytes);
    }
    else if (likely(bytes == last_stat_bytes))
    {
        return available_balance;
    }
    else
    {
        Int64 real_alloc_bytes = bytes - last_stat_bytes;
        // `alloc_bytes` is the number of byte that ReadLimiter has allocated.
        if (available_balance > 0)
        {
            auto can_alloc_bytes = std::min(real_alloc_bytes, available_balance);
            alloc_bytes += can_alloc_bytes;
            metricAllocBytes(type, can_alloc_bytes);
        }
        available_balance -= real_alloc_bytes;
    }
    last_stat_bytes = bytes;
    return available_balance;
}

void ReadLimiter::consumeBytes([[maybe_unused]] Int64 bytes)
{
    // Do nothing for read.
}

bool ReadLimiter::canGrant([[maybe_unused]] Int64 bytes)
{
    return getAvailableBalance() > 0;
}

void ReadLimiter::refillAndAlloc()
{
    // `available_balance` of `ReadLimiter` may be overdrawn.
    if (available_balance < 0)
    {
        // Limiter may not be called for a long time.
        // During this time, limiter can be refilled at most `max_refill_times` times and covers some overdraft.
        auto elapsed_duration = std::chrono::system_clock::now() - last_refill_time;
        UInt64 elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed_duration).count();
        // At least refill one time.
        Int64 max_refill_times = std::max(elapsed_ms, refill_period_ms) / refill_period_ms;
        Int64 max_refill_bytes = max_refill_times * refill_balance_per_period;
        Int64 can_alloc_bytes = std::min(-available_balance, max_refill_bytes);
        alloc_bytes += can_alloc_bytes;
        metricAllocBytes(type, can_alloc_bytes);
        available_balance = std::min(available_balance + max_refill_bytes, refill_balance_per_period);
    }
    else
    {
        available_balance = refill_balance_per_period;
    }
    last_refill_time = std::chrono::system_clock::now();

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

IORateLimiter::IORateLimiter(UInt64 update_read_info_period_ms_)
    : log(Logger::get("IORateLimiter"))
    , stop(false)
    , update_read_info_period_ms(update_read_info_period_ms_)
{}

IORateLimiter::~IORateLimiter()
{
    stop.store(true, std::memory_order_relaxed);
    if (auto_tune_and_get_read_info_thread.joinable())
    {
        auto_tune_and_get_read_info_thread.join();
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
    std::lock_guard lock(mtx);
    return is_background_thread ? bg_write_limiter : fg_write_limiter;
}

ReadLimiterPtr IORateLimiter::getReadLimiter()
{
    std::lock_guard lock(mtx);
    return is_background_thread ? bg_read_limiter : fg_read_limiter;
}

void IORateLimiter::updateConfig(Poco::Util::AbstractConfiguration & config_)
{
    IORateLimitConfig new_io_config;
    if (!readConfig(config_, new_io_config))
    {
        return;
    }
    std::lock_guard lock(mtx);
    updateReadLimiter(io_config.getBgReadMaxBytesPerSec(), io_config.getFgReadMaxBytesPerSec());
    updateWriteLimiter(io_config.getBgWriteMaxBytesPerSec(), io_config.getFgWriteMaxBytesPerSec());
}

bool IORateLimiter::readConfig(Poco::Util::AbstractConfiguration & config_, IORateLimitConfig & new_io_config)
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
    LOG_INFO(log, "storage.io_rate_limit is changed: {} => {}", io_config.toString(), new_io_config.toString());
    io_config = new_io_config;
    return true;
}

void IORateLimiter::updateReadLimiter(Int64 bg_bytes, Int64 fg_bytes)
{
    LOG_INFO(log, "updateReadLimiter: bg_bytes {} fg_bytes {}", bg_bytes, fg_bytes);
    auto get_bg_read_io_statistic = [&]() {
        return read_info.bg_read_bytes.load(std::memory_order_relaxed);
    };
    auto get_fg_read_io_statistic = [&]() {
        return read_info.fg_read_bytes.load(std::memory_order_relaxed);
    };

    if (bg_bytes == 0)
    {
        bg_read_limiter = nullptr;
    }
    else if (bg_read_limiter == nullptr) // bg_bytes != 0 && bg_read_limiter == nullptr
    {
        bg_read_limiter = std::make_shared<ReadLimiter>(get_bg_read_io_statistic, bg_bytes, LimiterType::BG_READ);
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
        fg_read_limiter = std::make_shared<ReadLimiter>(get_fg_read_io_statistic, fg_bytes, LimiterType::FG_READ);
    }
    else // fg_bytes != 0 && fg_read_limiter != nullptr
    {
        fg_read_limiter->updateMaxBytesPerSec(fg_bytes);
    }
}

void IORateLimiter::updateWriteLimiter(Int64 bg_bytes, Int64 fg_bytes)
{
    LOG_INFO(log, "updateWriteLimiter: bg_bytes {} fg_bytes {}", bg_bytes, fg_bytes);
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
    LOG_INFO(log, "bg_thread_ids {} => {}", bg_thread_ids.size(), bg_thread_ids);
}

Int64 IORateLimiter::getReadBytes(const std::string & fname [[maybe_unused]]) const
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
            LOG_WARNING(log, "readTaskIOInfo: {} is invalid.", s);
            continue;
        }
        if (values[0] == "read_bytes")
        {
            boost::algorithm::trim(values[1]);
            read_bytes = std::stoll(values[1]);
        }
    }
    if (read_bytes == -1)
    {
        auto msg = fmt::format("read_bytes: {}. Invalid result.", read_bytes);
        LOG_ERROR(log, msg);
        throw Exception(msg, ErrorCodes::UNKNOWN_EXCEPTION);
    }
    return read_bytes;
#else
    return 0;
#endif
}

void IORateLimiter::getCurrentIOInfo()
{
    static const pid_t pid = getpid();

    // Read read info of each background threads.
    Int64 bg_read_bytes_tmp{0};
    for (pid_t tid : bg_thread_ids)
    {
        const std::string thread_io_fname = fmt::format("/proc/{}/task/{}/io", pid, tid);
        Int64 read_bytes;
        read_bytes = getReadBytes(thread_io_fname);
        bg_read_bytes_tmp += read_bytes;
    }
    read_info.bg_read_bytes.store(bg_read_bytes_tmp, std::memory_order_relaxed);

    // Read read info of this process.
    static const std::string proc_io_fname = fmt::format("/proc/{}/io", pid);
    Int64 fg_read_bytes_tmp{getReadBytes(proc_io_fname) - bg_read_bytes_tmp};
    read_info.fg_read_bytes.store(std::max(0, fg_read_bytes_tmp), std::memory_order_relaxed);
}

void IORateLimiter::setStop()
{
    std::lock_guard lock(mtx);
    if (bg_write_limiter != nullptr)
    {
        auto sz = bg_write_limiter->setStop();
        LOG_DEBUG(log, "bg_write_limiter setStop request size {}", sz);
    }
    if (fg_write_limiter != nullptr)
    {
        auto sz = fg_write_limiter->setStop();
        LOG_DEBUG(log, "fg_write_limiter setStop request size {}", sz);
    }
    if (bg_read_limiter != nullptr)
    {
        auto sz = bg_read_limiter->setStop();
        LOG_DEBUG(log, "bg_read_limiter setStop request size {}", sz);
    }
    if (fg_read_limiter != nullptr)
    {
        auto sz = fg_read_limiter->setStop();
        LOG_DEBUG(log, "fg_read_limiter setStop request size {}", sz);
    }
}

void IORateLimiter::runAutoTune()
{
    auto auto_tune_and_get_read_info_worker = [&]() {
        using time_point = std::chrono::time_point<std::chrono::system_clock>;
        using clock = std::chrono::system_clock;
        time_point auto_tune_time = clock::now();
        time_point update_read_info_time = auto_tune_time;
        while (!stop.load(std::memory_order_relaxed))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(update_read_info_period_ms));
            auto now_time_point = clock::now();
            if ((io_config.auto_tune_sec > 0)
                && (now_time_point - auto_tune_time >= std::chrono::seconds(io_config.auto_tune_sec)))
            {
                autoTune();
                auto_tune_time = now_time_point;
            }
            if ((bg_read_limiter || fg_read_limiter)
                && likely(
                    now_time_point - update_read_info_time >= std::chrono::milliseconds(update_read_info_period_ms)))
            {
                getCurrentIOInfo();
                update_read_info_time = now_time_point;
            }
        }
    };
    auto_tune_and_get_read_info_thread = std::thread(auto_tune_and_get_read_info_worker);
}

std::unique_ptr<IOLimitTuner> IORateLimiter::createIOLimitTuner()
{
    WriteLimiterPtr bg_write, fg_write;
    ReadLimiterPtr bg_read, fg_read;
    IORateLimitConfig t_io_config;
    {
        std::lock_guard lock(mtx);
        bg_write = bg_write_limiter;
        fg_write = fg_write_limiter;
        bg_read = bg_read_limiter;
        fg_read = fg_read_limiter;
        t_io_config = io_config;
    }
    return std::make_unique<IOLimitTuner>(
        bg_write != nullptr ? std::make_unique<LimiterStat>(bg_write->getStat()) : nullptr,
        fg_write != nullptr ? std::make_unique<LimiterStat>(fg_write->getStat()) : nullptr,
        bg_read != nullptr ? std::make_unique<LimiterStat>(bg_read->getStat()) : nullptr,
        fg_read != nullptr ? std::make_unique<LimiterStat>(fg_read->getStat()) : nullptr,
        t_io_config);
}

void IORateLimiter::autoTune()
{
    try
    {
        auto tuner = createIOLimitTuner();
        auto tune_result = tuner->tune();
        if (tune_result.read_tuned)
        {
            std::lock_guard lock(mtx);
            updateReadLimiter(tune_result.max_bg_read_bytes_per_sec, tune_result.max_fg_read_bytes_per_sec);
        }
        if (tune_result.write_tuned)
        {
            std::lock_guard lock(mtx);
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
    const IORateLimitConfig & io_config_)
    : bg_write_stat(std::move(bg_write_stat_))
    , fg_write_stat(std::move(fg_write_stat_))
    , bg_read_stat(std::move(bg_read_stat_))
    , fg_read_stat(std::move(fg_read_stat_))
    , io_config(io_config_)
    , log(Logger::get())
{}

IOLimitTuner::TuneResult IOLimitTuner::tune() const
{
    if (limiterCount() < 2)
    {
        return {0, 0, false, 0, 0, false};
    }
    if (bg_write_stat)
    {
        LOG_DEBUG(log, "bg_write_stat => {}", bg_write_stat->toString());
    }
    if (fg_write_stat)
    {
        LOG_DEBUG(log, "fg_write_stat => {}", fg_write_stat->toString());
    }
    if (bg_read_stat)
    {
        LOG_DEBUG(log, "bg_read_stat => {}", bg_read_stat->toString());
    }
    if (fg_read_stat)
    {
        LOG_DEBUG(log, "fg_read_stat => {}", fg_read_stat->toString());
    }

    auto [max_read_bytes_per_sec, max_write_bytes_per_sec, rw_tuned] = tuneReadWrite();
    auto [max_bg_read_bytes_per_sec, max_fg_read_bytes_per_sec, read_tuned] = tuneRead(max_read_bytes_per_sec);
    auto [max_bg_write_bytes_per_sec, max_fg_write_bytes_per_sec, write_tuned] = tuneWrite(max_write_bytes_per_sec);
    if (rw_tuned || read_tuned || write_tuned)
    {
        LOG_INFO(
            log,
            "tune_msg: bg_write {} => {} fg_write {} => {} bg_read {} => {} fg_read {} => {}",
            bg_write_stat != nullptr ? bg_write_stat->maxBytesPerSec() : 0,
            max_bg_write_bytes_per_sec,
            fg_write_stat != nullptr ? fg_write_stat->maxBytesPerSec() : 0,
            max_fg_write_bytes_per_sec,
            bg_read_stat != nullptr ? bg_read_stat->maxBytesPerSec() : 0,
            max_bg_read_bytes_per_sec,
            fg_read_stat != nullptr ? fg_read_stat->maxBytesPerSec() : 0,
            max_fg_read_bytes_per_sec);
    }

    return {
        .max_bg_read_bytes_per_sec = max_bg_read_bytes_per_sec,
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
    TuneInfo write_info(
        maxWriteBytesPerSec(),
        avgWriteBytesPerSec(),
        writeWatermark(),
        io_config.getWriteMaxBytesPerSec());
    LOG_INFO(log, "read_tune_info => {} write_tune_info => {}", read_info.toString(), write_info.toString());
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
std::tuple<Int64, Int64, bool> IOLimitTuner::tuneBgFg(
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
    LOG_INFO(log, "bg_tune_info => {} fg_tune_info => {}", bg_info.toString(), fg_info.toString());
    auto [tuned_bg_max_bytes_per_sec, tuned_fg_max_bytes_per_sec, has_tuned] = tune(bg_info, fg_info);
    tuned_bg_max_bytes_per_sec
        = max_bytes_per_sec * tuned_bg_max_bytes_per_sec / (tuned_bg_max_bytes_per_sec + tuned_fg_max_bytes_per_sec);
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
            has_tuned
                = calculate(max_bytes_per_sec1, max_bytes_per_sec2, t1.config_max_bytes_per_sec - max_bytes_per_sec1);
        }
        else if (max_bytes_per_sec2 < t2.config_max_bytes_per_sec)
        {
            has_tuned
                = calculate(max_bytes_per_sec2, max_bytes_per_sec1, t2.config_max_bytes_per_sec - max_bytes_per_sec2);
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

IOLimitTuner::Watermark IOLimitTuner::getWatermark(const LimiterStatUPtr & fg, const LimiterStatUPtr & bg, int pct)
    const
{
    // Take `bg_read` and `fg_read` for example:
    // 1. Both `max_bg_read_bytes_per_sec` and `max_fg_read_bytes_per_sec` are less than `io_config.min_bytes_per_sec`.
    // 2. `bg_read` runs out of the bandwidth quota, but `fg_read`'s bandwidth quota has not been used.
    // 3. The usage rate of read is `(max_bg_read_bytes_per_sec + 0 ) / (max_bg_read_bytes_per_sec + max_fg_read_bytes_per_sec)`, about 50%.
    // 4. 50% is less than `io_config.medium_pct`(60% by default), so watermark is `LOW`.
    // 5. The `LOW` watermark means that bandwidth quota of read is sufficient since the usage rate is less than 60%, so it is unnessary to increase its bandwidth quota by decreasing the bandwidth quota of write.
    // 6. `bg_read` will only try to increase its bandwidth quota by decreasing the bandwidth quota of `fg_read`.
    // 7. However, `fg_read` is too small to decrease, so `bg_read` cannot be increased neither.
    // 8. To avoid the bad case above, if the bandwidth quota we want to decrease is too small, returning the greater watermark and try to tune bandwidth between read and write.
    if (fg != nullptr && bg != nullptr)
    {
        auto fg_wm = getWatermark(fg->pct());
        auto bg_wm = getWatermark(bg->pct());
        auto fg_mbps = fg->maxBytesPerSec();
        auto bg_mbps = bg->maxBytesPerSec();
        // `fg` needs more bandwidth, but `bg`'s bandwidth is small.
        if (fg_wm > bg_wm && bg_mbps <= io_config.min_bytes_per_sec * 2)
        {
            return fg_wm;
        }
        // `bg_read` needs more bandwidth, but `fg_read`'s bandwidth is small.
        else if (bg_wm > fg_wm && fg_mbps <= io_config.min_bytes_per_sec * 2)
        {
            return bg_wm;
        }
    }
    return getWatermark(pct);
}
} // namespace DB

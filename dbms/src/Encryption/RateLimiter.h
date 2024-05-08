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

#pragma once

#include <Common/Stopwatch.h>
#include <Common/nocopyable.h>
#include <Server/StorageConfigParser.h>
#include <fmt/core.h>
#include <fmt/std.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <magic_enum.hpp>
#include <mutex>
#include <thread>

// TODO: separate IO utility(i.e. FileProvider, RateLimiter) from Encryption directory
namespace Poco::Util
{
class AbstractConfiguration;
}
namespace DB
{
class LimiterStat;
class IOLimitTuner;

enum class LimiterType
{
    UNKNOW = 0,
    FG_WRITE = 1,
    BG_WRITE = 2,
    FG_READ = 3,
    BG_READ = 4,
};

/// ReadInfo is used to store IO information.
/// bg_read_bytes is the bytes of the background read.
/// fg_read_bytes is the bytes of the foreground read.
struct ReadInfo
{
    std::atomic<Int64> bg_read_bytes;
    std::atomic<Int64> fg_read_bytes;

    ReadInfo()
        : bg_read_bytes(0)
        , fg_read_bytes(0)
    {}

    std::string toString() const
    {
        return fmt::format("fg_read_bytes {} bg_read_bytes {}", fg_read_bytes, bg_read_bytes);
    }
};

// WriteLimiter is to control write rate (bytes per second).
// Because of the storage engine is append-only, the amount of data written by the storage engine
// is equal to the amount of data written to the disk by the operating system. So, WriteLimiter
// can limit the write request without any external dependencies.
//
// Constructor parameters:
//
// `rate_limit_per_sec_` controls the total write rate in bytes per second, 0 means no limit.
//
// `type_` is the type of this limiter. It is use for metrics.
//
// `refill_period_us` controls how often balance are refilled. For example, when rate_limit_per_sec_
// is set to 10MB/s and refill_period_us is set to 100ms, then 1MB is refilled every 100ms internally.
// Larger value can lead to burstier writes while smaller value introduces more CPU overhead. The default
// should work for most cases.
class WriteLimiter
{
public:
    WriteLimiter(Int64 rate_limit_per_sec_, LimiterType type_, UInt64 refill_period_ms_ = 100);

    virtual ~WriteLimiter();

    // `request()` is the main interface used by clients.
    // It receives the requested balance as the parameter,
    // and blocks until the request balance is satisfied.
    void request(Int64 bytes);

    // just for test purpose
    inline UInt64 getTotalBytesThrough() const
    {
        return available_balance < 0 ? alloc_bytes - available_balance : alloc_bytes;
    }

    LimiterStat getStat();

    void updateMaxBytesPerSec(Int64 max_bytes_per_sec);

    size_t setStop();
#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    virtual bool canGrant(Int64 bytes);
    virtual void consumeBytes(Int64 bytes);
    virtual void refillAndAlloc();

    inline Int64 calculateRefillBalancePerPeriod(Int64 rate_limit_per_sec_) const
    {
        auto refill_period_per_second = std::max(1, 1000 / refill_period_ms);
        return rate_limit_per_sec_ / refill_period_per_second;
    }

    // Just for test
    size_t pendingCount()
    {
        std::lock_guard lock(request_mutex);
        return req_queue.size();
    }

    // used to represent pending request
    struct Request
    {
        explicit Request(Int64 bytes)
            : remaining_bytes(bytes)
            , bytes(bytes)
            , granted(false)
        {}
        Int64 remaining_bytes;
        Int64 bytes;
        std::condition_variable cv;
        bool granted;
    };

    UInt64 refill_period_ms;
    AtomicStopwatch refill_stop_watch;

    Int64 refill_balance_per_period;
    Int64 available_balance;

    bool stop;
    std::condition_variable exit_cv;
    UInt32 requests_to_wait;

    using RequestQueue = std::deque<Request *>;
    RequestQueue req_queue;

    std::mutex request_mutex;

    const LimiterType type;

    Stopwatch stat_stop_watch;
    UInt64 alloc_bytes;
    LoggerPtr log;
};

using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;

// ReadLimiter is to control read rate (bytes per second).
// Because of the page cache, the amount of data read by the storage engine
// is NOT equal to the amount of data read from the disk by the operating system.
// So, ReadLimiter need some external dependencies to obtain the amount of data actually read.
// In this implementation, ReadLimiter obtain the amount of data actually read from the /proc filesystem:
// /proc/<pid>/io and /proc/<pid>/task<tid>/io.
//
// Constructor parameters:
//
// `get_read_bytes_` is the function that obtain the amount of data from `getCurrentIOInfo()` which read from /proc filesystem.
//
// Other parameters are the same as WriteLimiter.
class ReadLimiter : public WriteLimiter
{
public:
    ReadLimiter(
        std::function<Int64()> get_read_bytes_,
        Int64 rate_limit_per_sec_,
        LimiterType type_,
        UInt64 refill_period_ms_ = 100);

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif

    void refillAndAlloc() override;
    void consumeBytes(Int64 bytes) override;
    bool canGrant(Int64 bytes) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Int64 getAvailableBalance();

    std::function<Int64()> get_read_bytes;
    Int64 last_stat_bytes;
    std::chrono::time_point<std::chrono::system_clock> last_refill_time;
};

using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

// IORateLimiter is the wrapper of WriteLimiter and ReadLimiter.
// Currently, It supports four limiter type: background write, foreground write, background read and foreground read.
//
// Constructor parameters:
//
// `update_read_info_period_ms` is the interval between calling getCurrentIOInfo. Default is 30ms.
class IORateLimiter
{
public:
    explicit IORateLimiter(UInt64 update_read_info_period_ms_ = 30);
    ~IORateLimiter();

    WriteLimiterPtr getWriteLimiter();
    ReadLimiterPtr getReadLimiter();
    void init(Poco::Util::AbstractConfiguration & config_);
    void updateConfig(Poco::Util::AbstractConfiguration & config_);

    void setBackgroundThreadIds(std::vector<pid_t> thread_ids);

    void setStop();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Int64 getReadBytes(const std::string & fname);
    void getCurrentIOInfo();

    std::unique_ptr<IOLimitTuner> createIOLimitTuner();
    void autoTune();
    void runAutoTune();
    // readConfig return true if need to update limiter.
    bool readConfig(Poco::Util::AbstractConfiguration & config_, StorageIORateLimitConfig & new_io_config);
    void updateReadLimiter(Int64 bg_bytes, Int64 fg_bytes);
    void updateWriteLimiter(Int64 bg_bytes, Int64 fg_bytes);

    StorageIORateLimitConfig io_config;
    WriteLimiterPtr bg_write_limiter;
    WriteLimiterPtr fg_write_limiter;
    ReadLimiterPtr bg_read_limiter;
    ReadLimiterPtr fg_read_limiter;
    std::mutex mtx;

    std::mutex bg_thread_ids_mtx;
    std::vector<pid_t> bg_thread_ids;

    LoggerPtr log;

    std::atomic<bool> stop;
    std::thread auto_tune_and_get_read_info_thread;
    ReadInfo read_info;
    const UInt64 update_read_info_period_ms;

    // Noncopyable and nonmovable.
    DISALLOW_COPY_AND_MOVE(IORateLimiter);
};

class LimiterStat
{
public:
    LimiterStat(UInt64 alloc_bytes_, UInt64 elapsed_ms_, UInt64 refill_period_ms_, Int64 refill_bytes_per_period_)
        : alloc_bytes(alloc_bytes_)
        , elapsed_ms(elapsed_ms_)
        , refill_period_ms(refill_period_ms_)
        , refill_bytes_per_period(refill_bytes_per_period_)
    {
        assert(refill_period_ms > 0);
        assert(refill_bytes_per_period > 0);
        assert(elapsed_ms >= refill_period_ms);
    }

    String toString() const
    {
        return fmt::format(
            "alloc_bytes {} elapsed_ms {} refill_period_ms {} refill_bytes_per_period {} avg_bytes_per_sec {} "
            "max_bytes_per_sec {} pct {}",
            alloc_bytes,
            elapsed_ms,
            refill_period_ms,
            refill_bytes_per_period,
            avgBytesPerSec(),
            maxBytesPerSec(),
            pct());
    }

    Int64 avgBytesPerSec() const { return alloc_bytes * 1000 / elapsed_ms; }
    Int64 maxBytesPerSec() const { return refill_bytes_per_period * 1000 / refill_period_ms; }
    Int32 pct() const { return avgBytesPerSec() * 100 / maxBytesPerSec(); }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    UInt64 alloc_bytes;
    UInt64 elapsed_ms;
    UInt64 refill_period_ms;
    Int64 refill_bytes_per_period;
};

using LimiterStatUPtr = std::unique_ptr<LimiterStat>;

// IOLimitTuner will
class IOLimitTuner
{
public:
    IOLimitTuner(
        LimiterStatUPtr bg_write_stat_,
        LimiterStatUPtr fg_write_stat_,
        LimiterStatUPtr bg_read_stat_,
        LimiterStatUPtr fg_read_stat_,
        const StorageIORateLimitConfig & io_config_);

    String toString() const
    {
        return fmt::format(
            "bg_write {} fg_write {} bg_read {} fg_read {} io_config {}",
            bg_write_stat ? bg_write_stat->toString() : "null",
            fg_write_stat ? fg_write_stat->toString() : "null",
            bg_read_stat ? bg_read_stat->toString() : "null",
            fg_read_stat ? fg_read_stat->toString() : "null",
            io_config.toString());
    }

    struct TuneResult
    {
        Int64 max_bg_read_bytes_per_sec;
        Int64 max_fg_read_bytes_per_sec;
        bool read_tuned;

        Int64 max_bg_write_bytes_per_sec;
        Int64 max_fg_write_bytes_per_sec;
        bool write_tuned;

        String toString() const
        {
            return fmt::format(
                "max_bg_read_bytes_per_sec {} max_fg_read_bytes_per_sec {} read_tuned {} max_bg_write_bytes_per_sec {} "
                "max_fg_write_bytes_per_sec {} write_tuned {}",
                max_bg_read_bytes_per_sec,
                max_fg_read_bytes_per_sec,
                read_tuned,
                max_bg_write_bytes_per_sec,
                max_fg_write_bytes_per_sec,
                write_tuned);
        };

        bool operator==(const TuneResult & a) const
        {
            return max_bg_read_bytes_per_sec == a.max_bg_read_bytes_per_sec
                && max_fg_read_bytes_per_sec == a.max_fg_read_bytes_per_sec && read_tuned == a.read_tuned
                && max_bg_write_bytes_per_sec == a.max_bg_write_bytes_per_sec
                && max_fg_write_bytes_per_sec == a.max_fg_write_bytes_per_sec && write_tuned == a.write_tuned;
        }
    };

    TuneResult tune() const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    int limiterCount() const { return writeLimiterCount() + readLimiterCount(); }
    int writeLimiterCount() const { return (bg_write_stat != nullptr) + (fg_write_stat != nullptr); }
    int readLimiterCount() const { return (bg_read_stat != nullptr) + (fg_read_stat != nullptr); }

    // Background write and foreground write
    Int64 avgWriteBytesPerSec() const
    {
        return (bg_write_stat ? bg_write_stat->avgBytesPerSec() : 0)
            + (fg_write_stat ? fg_write_stat->avgBytesPerSec() : 0);
    }
    Int64 maxWriteBytesPerSec() const
    {
        return (bg_write_stat ? bg_write_stat->maxBytesPerSec() : 0)
            + (fg_write_stat ? fg_write_stat->maxBytesPerSec() : 0);
    }
    int writePct() const
    {
        auto max = maxWriteBytesPerSec();
        return max > 0 ? avgWriteBytesPerSec() * 100 / max : 0;
    }

    // Background read and foreground read
    Int64 avgReadBytesPerSec() const
    {
        return (bg_read_stat ? bg_read_stat->avgBytesPerSec() : 0)
            + (fg_read_stat ? fg_read_stat->avgBytesPerSec() : 0);
    }
    Int64 maxReadBytesPerSec() const
    {
        return (bg_read_stat ? bg_read_stat->maxBytesPerSec() : 0)
            + (fg_read_stat ? fg_read_stat->maxBytesPerSec() : 0);
    }
    int readPct() const
    {
        auto max = maxReadBytesPerSec();
        return max > 0 ? avgReadBytesPerSec() * 100 / max : 0;
    }

    // Watermark describes the I/O utilization roughly.
    enum class Watermark
    {
        Low = 1,
        Medium = 2,
        High = 3,
        Emergency = 4
    };
    Watermark writeWatermark() const { return getWatermark(fg_write_stat, bg_write_stat, writePct()); }
    Watermark readWatermark() const { return getWatermark(fg_read_stat, bg_read_stat, readPct()); }
    Watermark getWatermark(int pct) const;
    Watermark getWatermark(const LimiterStatUPtr & fg, const LimiterStatUPtr & bg, int pct) const;

    // Returns <max_read_bytes_per_sec, max_write_bytes_per_sec, has_tuned>
    std::tuple<Int64, Int64, bool> tuneReadWrite() const;
    // Retunes <bg, fg, has_tune>
    std::tuple<Int64, Int64, bool> tuneRead(Int64 max_bytes_per_sec) const;
    // Retunes <bg, fg, has_tune>
    std::tuple<Int64, Int64, bool> tuneWrite(Int64 max_bytes_per_sec) const;
    // <bg, fg, has_tune>
    std::tuple<Int64, Int64, bool> tuneBgFg(
        Int64 max_bytes_per_sec,
        const LimiterStatUPtr & bg,
        Int64 config_bg_max_bytes_per_sec,
        const LimiterStatUPtr & fg,
        Int64 config_fg_max_bytes_per_sec) const;
    // Returns true if to_add and to_sub are changed.
    bool calculate(Int64 & to_add, Int64 & to_sub, Int64 delta) const;

    struct TuneInfo
    {
        Int64 max_bytes_per_sec;
        Int64 avg_bytes_per_sec;
        Watermark watermark;

        Int64 config_max_bytes_per_sec;

        TuneInfo(Int64 max, Int64 avg, Watermark wm, Int64 config_max)
            : max_bytes_per_sec(max)
            , avg_bytes_per_sec(avg)
            , watermark(wm)
            , config_max_bytes_per_sec(config_max)
        {}

        String toString() const
        {
            return fmt::format(
                "max {} avg {} watermark {} config_max {}",
                max_bytes_per_sec,
                avg_bytes_per_sec,
                magic_enum::enum_name(watermark),
                config_max_bytes_per_sec);
        }
    };
    // <max_bytes_per_sec1, max_bytes_per_sec2, has_tuned>
    std::tuple<Int64, Int64, bool> tune(const TuneInfo & t1, const TuneInfo & t2) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LimiterStatUPtr bg_write_stat;
    LimiterStatUPtr fg_write_stat;
    LimiterStatUPtr bg_read_stat;
    LimiterStatUPtr fg_read_stat;
    StorageIORateLimitConfig io_config;
    LoggerPtr log;
};
} // namespace DB

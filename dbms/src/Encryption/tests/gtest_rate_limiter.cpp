#include <Common/Exception.h>
#include <Encryption/RateLimiter.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>
#include <thread>

#ifdef __linux__
#include <sys/syscall.h>
#endif

using namespace std::chrono_literals;

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace tests
{
TEST(WriteLimiter_test, Rate)
{
    srand((unsigned)time(NULL));
    auto write = [](const WriteLimiterPtr & write_limiter, UInt64 max_request_size) {
        AtomicStopwatch watch;
        while (watch.elapsedSeconds() < 4)
        {
            auto size = rand() % max_request_size + 1;
            write_limiter->request(size);
        }
    };

    for (int i = 1; i <= 16; i *= 2)
    {
        UInt64 target = i * 1024 * 10;
        // refill ten times every second
        auto write_limiter = std::make_shared<WriteLimiter>(target, LimiterType::UNKNOW, 100);
        AtomicStopwatch watch;
        std::vector<std::thread> threads;
        // create multiple threads to perform request command
        for (size_t num = 0; num < 10; num++)
        {
            std::thread thread(write, std::ref(write_limiter), target / 10);
            threads.push_back(std::move(thread));
        }
        for (auto & thread : threads)
            thread.join();
        auto elapsed = watch.elapsedSeconds();
        auto actual_rate = write_limiter->getTotalBytesThrough() / elapsed;
        // make sure that 0.8 * target <= actual_rate <= 1.25 * target
        // hint: the range [0.8, 1.25] is copied from rocksdb,
        // if tests fail, try to enlarge this range.
        EXPECT_GE(actual_rate / target, 0.80);
        EXPECT_LE(actual_rate / target, 1.25);
    }
}

TEST(WriteLimiter_test, LimiterStat_NotLimit)
{
    WriteLimiter write_limiter(0, LimiterType::UNKNOW, 100);
    try
    {
        write_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }

    auto noop = []() { return 0; };
    ReadLimiter read_limiter(noop, 0, LimiterType::UNKNOW, 100);
    try
    {
        read_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }
}

TEST(WriteLimiter_test, LimiterStat)
{

    WriteLimiter write_limiter(1000, LimiterType::UNKNOW, 100);
    try
    {
        write_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }

    std::this_thread::sleep_for(100ms);

    auto stat = write_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, 0ul);
    ASSERT_GE(stat.elapsed_ms, 100ul);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), 0);
    ASSERT_EQ(stat.pct(), 0);

    try
    {
        write_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }

    write_limiter.request(100);

    std::this_thread::sleep_for(100ms);

    stat = write_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, 100ul);
    ASSERT_GE(stat.elapsed_ms, 100ul);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), 1000) << stat.toString();
    ASSERT_EQ(stat.pct(), 100) << stat.toString();

    static constexpr UInt64 alloc_bytes = 2047;
    for (int i = 0; i < 11; i++)
    {
        write_limiter.request(1 << i);
    }

    stat = write_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, alloc_bytes);
    ASSERT_GE(stat.elapsed_ms, alloc_bytes / 100 + 1);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), static_cast<Int64>(alloc_bytes * 1000 / stat.elapsed_ms)) << stat.toString();
    ASSERT_EQ(stat.pct(), static_cast<Int64>(alloc_bytes * 1000 / stat.elapsed_ms) * 100 / stat.maxBytesPerSec()) << stat.toString();
}

TEST(ReadLimiter_test, GetIOStatPeroid_2000us)
{
    Int64 consumed = 0;
    auto getStat = [&consumed]() { return consumed; };
    auto request = [&consumed](ReadLimiter & limiter, Int64 bytes) {
        limiter.request(bytes);
        consumed += bytes;
    };
    Int64 get_io_stat_period_us = 2000;
    auto waitRefresh = [&]() {
        std::chrono::microseconds sleep_time(get_io_stat_period_us + 1);
        std::this_thread::sleep_for(sleep_time);
    };

    using TimePointMS = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    Int64 bytes_per_sec = 1000;
    UInt64 refill_period_ms = 20;
    ReadLimiter limiter(getStat, bytes_per_sec, LimiterType::UNKNOW, get_io_stat_period_us, refill_period_ms);

    TimePointMS t0 = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    // Refill 20 every 20ms.
    ASSERT_EQ(limiter.getAvailableBalance(), 20);
    request(limiter, 1);
    ASSERT_EQ(limiter.getAvailableBalance(), 20);
    ASSERT_EQ(limiter.refreshAvailableBalance(), 19);
    request(limiter, 9);
    ASSERT_EQ(limiter.getAvailableBalance(), 19);
    waitRefresh();
    ASSERT_EQ(limiter.getAvailableBalance(), 10);
    request(limiter, 11);
    waitRefresh();
    ASSERT_EQ(limiter.getAvailableBalance(), -1);
    request(limiter, 50);
    TimePointMS t1 = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    UInt64 elasped = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    ASSERT_GE(elasped, refill_period_ms);
    ASSERT_EQ(limiter.getAvailableBalance(), 19);
    waitRefresh();
    ASSERT_EQ(limiter.getAvailableBalance(), -31);
    request(limiter, 1);
    TimePointMS t2 = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    elasped = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
    ASSERT_GE(elasped, 2 * refill_period_ms);
}

void testSetStop(bool stop, int blocked_thread_cnt)
{
    auto write_limiter = std::make_shared<WriteLimiter>(1000, LimiterType::UNKNOW, 100);
    // All the bytes are consumed in this request, and next refill time is about 100ms later.
    write_limiter->request(100);

    std::atomic<UInt32> counter{0};
    auto worker = [&]() {
        counter.fetch_add(1, std::memory_order_relaxed);
        write_limiter->request(1); 
    };
    std::vector<std::thread> threads;
    auto start = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    for (int i = 0; i < blocked_thread_cnt; i++)
    {
        // All threads are blocked inside limiter.
        threads.push_back(std::thread(worker));
    }
    
    if (stop)
    {
        // Wait threads to be scheduled.
        while (counter.load(std::memory_order_relaxed) < threads.size())
        {
            std::this_thread::sleep_for(1ms);
        }
        std::this_thread::sleep_for(10ms);  // Roughly wait for threads to request limiter.
        auto sz = write_limiter->setStop(); // Stop the limiter and notify threads that blocked inside limiter.
        ASSERT_EQ(sz, threads.size()) << sz;
    }

    for (auto & t : threads)
    {
        t.join();
    }
    auto end = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    auto elasped = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    if (stop)
    {
        // After setStop, threads that blocked inside limiter will be notified immediately.
        ASSERT_LT(elasped, 100) << elasped;
    }
    else
    {
        // If not setStop, threads that blocked inside limiter will be notified until timeout (about 100ms).
        ASSERT_GE(elasped, 100) << elasped;
    }
}

TEST(WriteLimiter_test, setStop)
{
    for (int i = 1; i < 128; i++)
    {
        testSetStop(false, i);
        testSetStop(true, i);
    }
}

TEST(ReadLimiter_test, LimiterStat)
{
    Int64 consumed = 0;
    auto getStat = [&consumed]() { return consumed; };
    auto request = [&consumed](ReadLimiter & limiter, Int64 bytes) {
        limiter.request(bytes);
        consumed += bytes;
    };
    Int64 get_io_stat_period_us = 2000;
    ReadLimiter read_limiter(getStat, 1000, LimiterType::UNKNOW, get_io_stat_period_us, 100);
    try
    {
        read_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }

    std::this_thread::sleep_for(100ms);

    auto stat = read_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, 0ul);
    ASSERT_GE(stat.elapsed_ms, 100ul);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), 0);
    ASSERT_EQ(stat.pct(), 0);

    try
    {
        read_limiter.getStat();
        ASSERT_FALSE(true); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }

    request(read_limiter, 100);

    std::this_thread::sleep_for(100ms);
    read_limiter.refreshAvailableBalance();

    stat = read_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, 100ul);
    ASSERT_GE(stat.elapsed_ms, 100ul);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), 1000) << stat.toString();
    ASSERT_EQ(stat.pct(), 100) << stat.toString();

    static constexpr UInt64 alloc_bytes = 2047;
    for (int i = 0; i < 11; i++)
    {
        request(read_limiter, 1 << i);
    }

    std::this_thread::sleep_for(100ms);
    read_limiter.refreshAvailableBalance();

    stat = read_limiter.getStat();
    ASSERT_EQ(stat.alloc_bytes, alloc_bytes);
    ASSERT_GE(stat.elapsed_ms, alloc_bytes / 100 + 1);
    ASSERT_EQ(stat.refill_period_ms, 100ul);
    ASSERT_EQ(stat.refill_bytes_per_period, 100);
    ASSERT_EQ(stat.maxBytesPerSec(), 1000);
    ASSERT_EQ(stat.avgBytesPerSec(), static_cast<Int64>(alloc_bytes * 1000 / stat.elapsed_ms)) << stat.toString();
    ASSERT_EQ(stat.pct(), static_cast<Int64>(alloc_bytes * 1000 / stat.elapsed_ms) * 100 / stat.maxBytesPerSec()) << stat.toString();
}

#ifdef __linux__
TEST(IORateLimiter_test, IOStat)
{
    IORateLimiter io_rate_limiter;

    // Default value
    ASSERT_EQ(io_rate_limiter.bg_write_limiter, nullptr);
    ASSERT_EQ(io_rate_limiter.fg_write_limiter, nullptr);
    ASSERT_EQ(io_rate_limiter.bg_read_limiter, nullptr);
    ASSERT_EQ(io_rate_limiter.fg_read_limiter, nullptr);

    std::string fname = "/tmp/rate_limit_io_stat_test";
    int fd = ::open(fname.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0666);
    ASSERT_GT(fd, 0) << strerror(errno);
    std::unique_ptr<int, std::function<void(int * fd)>> defer_close(&fd, [](int * fd) { ::close(*fd); });

    void * buf = nullptr;
    int buf_size = 4096;
    int ret = ::posix_memalign(&buf, buf_size, buf_size);
    ASSERT_EQ(ret, 0) << strerror(errno);
    std::unique_ptr<void, std::function<void(void *)>> defer_free(buf, [](void * p) { ::free(p); });

    ssize_t n = ::pwrite(fd, buf, buf_size, 0);
    ASSERT_EQ(n, buf_size) << strerror(errno);

    n = ::pread(fd, buf, buf_size, 0);
    ASSERT_EQ(n, buf_size) << strerror(errno);

    //int ret = ::fsync(fd);
    //ASSERT_EQ(ret, 0) << strerror(errno);

    auto io_info = io_rate_limiter.getCurrentIOInfo();
    ASSERT_GE(io_info.total_write_bytes, buf_size);
    ASSERT_GE(io_info.total_read_bytes, buf_size);
}

TEST(IORateLimiter_test, IOStatMultiThread)
{
    std::mutex bg_pids_mtx;
    std::vector<pid_t> bg_pids;
    auto addBgPid = [&](pid_t tid) {
        std::lock_guard lock(bg_pids_mtx);
        bg_pids.push_back(tid);
    };

    constexpr int buf_size = 4096;
    constexpr int bg_thread_count = 8;
    constexpr int fg_thread_count = 8;
    std::atomic<int> finished_count(0);
    std::atomic<bool> stop(false);
    auto write = [&](int id, bool is_bg) {
        if (is_bg)
        {
            addBgPid(syscall(SYS_gettid));
        }
        std::string fname = "/tmp/rate_limit_io_stat_test_" + std::to_string(id) + (is_bg ? "_bg" : "_fg");
        int fd = ::open(fname.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0666);
        ASSERT_GT(fd, 0) << strerror(errno);
        std::unique_ptr<int, std::function<void(int * fd)>> defer_close(&fd, [](int * fd) { ::close(*fd); });

        void * buf = nullptr;
        int ret = ::posix_memalign(&buf, buf_size, buf_size);
        std::unique_ptr<void, std::function<void(void *)>> auto_free(buf, [](void * p) { free(p); });
        ASSERT_EQ(ret, 0) << strerror(errno);

        ssize_t n = ::pwrite(fd, buf, buf_size, 0);
        ASSERT_EQ(n, buf_size) << strerror(errno);

        n = ::pread(fd, buf, buf_size, 0);
        ASSERT_EQ(n, buf_size) << strerror(errno);

        finished_count++;
        while (!stop.load())
        {
            std::this_thread::sleep_for(1s);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < bg_thread_count; i++)
    {
        threads.push_back(std::thread(write, i, true));
    }

    for (int i = 0; i < fg_thread_count; i++)
    {
        threads.push_back(std::thread(write, i, false));
    }

    while (finished_count.load() < bg_thread_count + fg_thread_count)
    {
        std::this_thread::sleep_for(1s);
    }

    IORateLimiter io_rate_limiter;
    io_rate_limiter.setBackgroundThreadIds(bg_pids);
    auto io_info = io_rate_limiter.getCurrentIOInfo();
    std::cout << io_info.toString() << std::endl;
    ASSERT_GE(io_info.total_read_bytes, buf_size * (bg_thread_count + fg_thread_count));
    ASSERT_GE(io_info.total_write_bytes, buf_size * (bg_thread_count + fg_thread_count));
    ASSERT_GE(io_info.bg_read_bytes, buf_size * bg_thread_count);
    ASSERT_GE(io_info.bg_write_bytes, buf_size * bg_thread_count);
    stop.store(true);

    for (auto & t : threads)
    {
        t.join();
    }
}
#endif

LimiterStatUPtr createLimiterStat(UInt64 alloc_bytes, UInt64 elapsed_ms, UInt64 refill_period_ms, Int64 refill_bytes_per_period)
{
    return std::make_unique<LimiterStat>(alloc_bytes, elapsed_ms, refill_period_ms, refill_bytes_per_period);
}

TEST(IOLimitTuner_test, Watermark)
{
    StorageIORateLimitConfig io_config;
    io_config.emergency_pct = 95;
    io_config.high_pct = 80;
    io_config.medium_pct = 60;

    IOLimitTuner tuner(nullptr, nullptr, nullptr, nullptr, io_config);

    for (int i = 0; i < 60; i++)
    {
        ASSERT_EQ(tuner.getWatermark(i), IOLimitTuner::Watermark::Low);
    }
    for (int i = 60; i < 80; i++)
    {
        ASSERT_EQ(tuner.getWatermark(i), IOLimitTuner::Watermark::Medium);
    }
    for (int i = 80; i < 95; i++)
    {
        ASSERT_EQ(tuner.getWatermark(i), IOLimitTuner::Watermark::High);
    }
    for (int i = 95; i <= 100; i++)
    {
        ASSERT_EQ(tuner.getWatermark(i), IOLimitTuner::Watermark::Emergency);
    }
}

TEST(IOLimitTuner_test, NotNeedTune)
{
    StorageIORateLimitConfig io_config;
    io_config.max_bytes_per_sec = 1000;

    auto assertTuner = [](const auto & tuner, int write_limiter_cnt, int read_limiter_cnt) {
        ASSERT_EQ(tuner.writeLimiterCount(), write_limiter_cnt);
        ASSERT_EQ(tuner.readLimiterCount(), read_limiter_cnt);
        ASSERT_EQ(tuner.limiterCount(), write_limiter_cnt + read_limiter_cnt);
        auto res = tuner.tune();
        ASSERT_FALSE(res.read_tuned);
        ASSERT_FALSE(res.write_tuned);

        ASSERT_EQ(tuner.avgWriteBytesPerSec(), write_limiter_cnt > 0 ? 1000 : 0);
        ASSERT_EQ(tuner.avgReadBytesPerSec(), read_limiter_cnt > 0 ? 1000 : 0);

        ASSERT_EQ(tuner.maxWriteBytesPerSec(), write_limiter_cnt > 0 ? 1000 : 0);
        ASSERT_EQ(tuner.maxReadBytesPerSec(), read_limiter_cnt ? 1000 : 0);

        ASSERT_EQ(tuner.writePct(), write_limiter_cnt ? 100 : 0);
        ASSERT_EQ(tuner.readPct(), read_limiter_cnt ? 100 : 0);
    };

    {
        IOLimitTuner tuner(nullptr, nullptr, nullptr, nullptr, io_config);
        assertTuner(tuner, 0, 0);
    }

    {
        IOLimitTuner tuner(createLimiterStat(100, 100, 100, 100), nullptr, nullptr, nullptr, io_config);
        assertTuner(tuner, 1, 0);
    }

    {
        IOLimitTuner tuner(nullptr, createLimiterStat(100, 100, 100, 100), nullptr, nullptr, io_config);
        assertTuner(tuner, 1, 0);
    }

    {
        IOLimitTuner tuner(nullptr, nullptr, createLimiterStat(100, 100, 100, 100), nullptr, io_config);
        assertTuner(tuner, 0, 1);
    }

    {
        IOLimitTuner tuner(nullptr, nullptr, nullptr, createLimiterStat(100, 100, 100, 100), io_config);
        assertTuner(tuner, 0, 1);
    }
}

template <typename T>
IOLimitTuner::Watermark watermarkOfBgWrite(const T & tuner) { return tuner.getWatermark(tuner.bg_write_stat->pct()); }

template <typename T>
IOLimitTuner::Watermark watermarkOfFgWrite(const T & tuner) { return tuner.getWatermark(tuner.fg_write_stat->pct()); }

template <typename T>
IOLimitTuner::Watermark watermarkOfBgRead(const T & tuner) { return tuner.getWatermark(tuner.bg_read_stat->pct()); }

template <typename T>
IOLimitTuner::Watermark watermarkOfFgRead(const T & tuner) { return tuner.getWatermark(tuner.fg_read_stat->pct()); }

void updateWatermarkPct(StorageIORateLimitConfig & io_config, int emergency, int high, int medium)
{
    io_config.emergency_pct = emergency;
    io_config.high_pct = high;
    io_config.medium_pct = medium;
}

void updateWeight(StorageIORateLimitConfig & io_config, int bg_write, int fg_write, int bg_read, int fg_read)
{
    io_config.bg_write_weight = bg_write;
    io_config.fg_write_weight = fg_write;
    io_config.bg_read_weight = bg_read;
    io_config.fg_read_weight = fg_read;
}

constexpr auto Low = IOLimitTuner::Watermark::Low;
constexpr auto Medium = IOLimitTuner::Watermark::Medium;
constexpr auto High = IOLimitTuner::Watermark::High;
constexpr auto Emergency = IOLimitTuner::Watermark::Emergency;

TEST(IOLimitTuner_test, Tune)
{
    StorageIORateLimitConfig io_config;
    io_config.max_bytes_per_sec = 2000;
    updateWatermarkPct(io_config, 90, 80, 50);
    io_config.min_bytes_per_sec = 10;

    struct TestArgument
    {
        UInt64 alloc1;
        UInt64 alloc2;
        Int64 max1;
        Int64 max2;
        IOLimitTuner::Watermark total_wm;
        IOLimitTuner::Watermark wm1;
        IOLimitTuner::Watermark wm2;
        IOLimitTuner::TuneResult excepted_res;
    };

    auto test1 = [&](const TestArgument & t) {
        updateWeight(io_config, 0, 0, 50, 50);
        auto stat1 = createLimiterStat(t.alloc1, 100, 100, t.max1);
        auto stat2 = createLimiterStat(t.alloc2, 100, 100, t.max2);
        IOLimitTuner tuner(nullptr, nullptr, std::move(stat1), std::move(stat2), io_config);
        ASSERT_EQ(tuner.readWatermark(), t.total_wm);
        ASSERT_EQ(watermarkOfBgRead(tuner), t.wm1);
        ASSERT_EQ(watermarkOfFgRead(tuner), t.wm2);
        auto res = tuner.tune();
        ASSERT_EQ(res, t.excepted_res) << "res: " << res.toString() << " excepted_res: " << t.excepted_res.toString();
    };

    auto test2 = [&](const TestArgument & t) {
        updateWeight(io_config, 0, 0, 50, 50);
        auto stat1 = createLimiterStat(t.alloc1, 100, 100, t.max1);
        auto stat2 = createLimiterStat(t.alloc2, 100, 100, t.max2);
        IOLimitTuner tuner(nullptr, nullptr, std::move(stat2), std::move(stat1), io_config);
        ASSERT_EQ(tuner.readWatermark(), t.total_wm);
        ASSERT_EQ(watermarkOfBgRead(tuner), t.wm2);
        ASSERT_EQ(watermarkOfFgRead(tuner), t.wm1);
        auto res = tuner.tune();
        auto excepted_res = t.excepted_res;
        std::swap(excepted_res.max_bg_read_bytes_per_sec, excepted_res.max_fg_read_bytes_per_sec);
        std::swap(excepted_res.max_bg_write_bytes_per_sec, excepted_res.max_fg_write_bytes_per_sec);
        ASSERT_EQ(res, excepted_res) << "res: " << res.toString() << " excepted_res: " << excepted_res.toString();
    };

    auto test3 = [&](const TestArgument & t) {
        updateWeight(io_config, 50, 50, 0, 0);
        auto stat1 = createLimiterStat(t.alloc1, 100, 100, t.max1);
        auto stat2 = createLimiterStat(t.alloc2, 100, 100, t.max2);
        IOLimitTuner tuner(std::move(stat1), std::move(stat2), nullptr, nullptr, io_config);
        ASSERT_EQ(tuner.writeWatermark(), t.total_wm);
        ASSERT_EQ(watermarkOfBgWrite(tuner), t.wm1);
        ASSERT_EQ(watermarkOfFgWrite(tuner), t.wm2);
        auto res = tuner.tune();
        auto excepted_res = t.excepted_res;
        std::swap(excepted_res.max_bg_read_bytes_per_sec, excepted_res.max_bg_write_bytes_per_sec);
        std::swap(excepted_res.max_fg_read_bytes_per_sec, excepted_res.max_fg_write_bytes_per_sec);
        std::swap(excepted_res.write_tuned, excepted_res.read_tuned);
        ASSERT_EQ(res, excepted_res) << "res: " << res.toString() << " excepted_res: " << excepted_res.toString();
    };

    auto test4 = [&](const TestArgument & t) {
        updateWeight(io_config, 50, 50, 0, 0);
        auto stat1 = createLimiterStat(t.alloc1, 100, 100, t.max1);
        auto stat2 = createLimiterStat(t.alloc2, 100, 100, t.max2);
        IOLimitTuner tuner(std::move(stat2), std::move(stat1), nullptr, nullptr, io_config);
        ASSERT_EQ(tuner.writeWatermark(), t.total_wm);
        ASSERT_EQ(watermarkOfBgWrite(tuner), t.wm2);
        ASSERT_EQ(watermarkOfFgWrite(tuner), t.wm1);
        auto res = tuner.tune();
        auto excepted_res = t.excepted_res;
        std::swap(excepted_res.max_bg_read_bytes_per_sec, excepted_res.max_fg_write_bytes_per_sec);
        std::swap(excepted_res.max_fg_read_bytes_per_sec, excepted_res.max_bg_write_bytes_per_sec);
        std::swap(excepted_res.write_tuned, excepted_res.read_tuned);
        ASSERT_EQ(res, excepted_res) << "res: " << res.toString() << " excepted_res: " << excepted_res.toString();
    };

    auto test5 = [&](const TestArgument & t) {
        updateWeight(io_config, 50, 0, 50, 0);
        auto stat1 = createLimiterStat(t.alloc1, 100, 100, t.max1);
        auto stat2 = createLimiterStat(t.alloc2, 100, 100, t.max2);
        IOLimitTuner tuner(std::move(stat1), nullptr, std::move(stat2), nullptr, io_config);
        ASSERT_EQ(watermarkOfBgWrite(tuner), tuner.writeWatermark());
        ASSERT_EQ(watermarkOfBgRead(tuner), tuner.readWatermark());
        auto res = tuner.tune();
        auto excepted_res = t.excepted_res;
        std::swap(excepted_res.max_bg_write_bytes_per_sec, excepted_res.max_bg_read_bytes_per_sec);
        std::swap(excepted_res.max_bg_read_bytes_per_sec, excepted_res.max_fg_read_bytes_per_sec);
        excepted_res.write_tuned = excepted_res.read_tuned;
        ASSERT_EQ(res, excepted_res) << "res: " << res.toString() << " excepted_res: " << excepted_res.toString();
    };

    std::vector<TestArgument> args;
    // Both Low not need tune.
    args.push_back({10, 10, 100, 100, Low, Low, Low, {1000, 1000, false, 0, 0, false}});
    // Both Medium not need tune.
    args.push_back({60, 60, 100, 100, Medium, Medium, Medium, {1000, 1000, false, 0, 0, false}});
    // Both High not need tune.
    args.push_back({85, 85, 100, 100, High, High, High, {1000, 1000, false, 0, 0, false}});
    // Both Emergency
    args.push_back({48, 144, 50, 150, Emergency, Emergency, Emergency, {750, 1250, true, 0, 0, false}});
    // Low + Medium
    args.push_back({10, 60, 100, 100, Low, Low, Medium, {550, 1450, true, 0, 0, false}});
    // Low + High
    args.push_back({10, 82, 100, 100, Low, Low, High, {550, 1450, true, 0, 0, false}});
    // Low + Emergency
    args.push_back({10, 92, 100, 100, Medium, Low, Emergency, {550, 1450, true, 0, 0, false}});
    // Medium + High
    args.push_back({60, 82, 100, 100, Medium, Medium, High, {800, 1200, true, 0, 0, false}});
    // High + Emergency
    args.push_back({82, 92, 100, 100, High, High, Emergency, {910, 1090, true, 0, 0, false}});
    for (const auto & t : args)
    {
        test1(t);
        test2(t);
        test3(t);
        test4(t);
        test5(t);
    }
}

} // namespace tests
} // namespace DB

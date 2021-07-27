#include <Encryption/RateLimiter.h>
#include <gtest/gtest.h>

#include <ctime>
#include <thread>
#include <unistd.h>
#include <fcntl.h>

#ifdef __linux__
#include <sys/syscall.h>
#endif

using namespace std::chrono_literals;

namespace DB
{
namespace tests
{
TEST(RateLimiter_test, Rate)
{
    srand((unsigned)time(NULL));
    auto write = [](const RateLimiterPtr & rate_limiter, UInt64 max_request_size) {
        AtomicStopwatch watch;
        while (watch.elapsedSeconds() < 4)
        {
            auto size = rand() % max_request_size + 1;
            rate_limiter->request(size);
        }
    };

    for (int i = 1; i <= 16; i *= 2)
    {
        UInt64 target = i * 1024 * 10;
        // refill ten times every second
        auto rate_limiter = std::make_shared<RateLimiter>(nullptr, target, LimiterType::UNKNOW, 100);
        AtomicStopwatch watch;
        std::vector<std::thread> threads;
        // create multiple threads to perform request command
        for (size_t num = 0; num < 10; num++)
        {
            std::thread thread(write, std::ref(rate_limiter), target / 10);
            threads.push_back(std::move(thread));
        }
        for (auto & thread : threads)
            thread.join();
        auto elapsed = watch.elapsedSeconds();
        auto actual_rate = rate_limiter->getTotalBytesThrough() / elapsed;
        // make sure that 0.8 * target <= actual_rate <= 1.25 * target
        // hint: the range [0.8, 1.25] is copied from rocksdb,
        // if tests fail, try to enlarge this range.
        EXPECT_GE(actual_rate / target, 0.80);
        EXPECT_LE(actual_rate / target, 1.25);
    }
}

TEST(ReadLimiter_test, GetIOStatPeroid_2000us)
{
    Int64 consumed = 0;
    auto getStat = [&consumed]()
    {
        return consumed;
    };
    auto request = [&consumed](ReadLimiter& limiter, Int64 bytes)
    {
        limiter.request(bytes);
        consumed += bytes;
    };
    Int64 get_io_stat_period_us = 2000;
    auto waitRefresh = [&]()
    {
        std::chrono::microseconds sleep_time(get_io_stat_period_us + 1);
        std::this_thread::sleep_for(sleep_time);
    };

    using TimePointMS = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    Int64 bytes_per_sec = 1000;
    UInt64 refill_period_ms = 20;
    ReadLimiter limiter(getStat, nullptr, bytes_per_sec, LimiterType::UNKNOW, get_io_stat_period_us, refill_period_ms);

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

#ifdef __linux__
TEST(ReadLimiter_test, IOStat)
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
    std::unique_ptr<int, std::function<void(int* fd)>> defer_close(&fd, [](int* fd) { ::close(*fd); });
    
    void* buf = nullptr;
    int buf_size = 4096;
    int ret = ::posix_memalign(&buf, buf_size, buf_size);
    ASSERT_EQ(ret, 0) << strerror(errno);
    std::unique_ptr<void, std::function<void(void*)>> defer_free(buf, [](void* p) { ::free(p); });

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

TEST(ReadLimiter_test, IOStatMultiThread)
{
    std::mutex bg_pids_mtx;
    std::vector<pid_t> bg_pids;
    auto addBgPid = [&](pid_t tid)
    {
        std::lock_guard lock(bg_pids_mtx);
        bg_pids.push_back(tid);
    };
    
    constexpr int buf_size = 4096;
    constexpr int bg_thread_count = 8;
    constexpr int fg_thread_count = 8;
    std::atomic<int> finished_count(0);
    std::atomic<bool> stop(false);
    auto write = [&](int id, bool is_bg)
    {
        if (is_bg)
        {
            addBgPid(syscall(SYS_gettid));
        }
        std::string fname = "/tmp/rate_limit_io_stat_test_" + std::to_string(id) + (is_bg ? "_bg" : "_fg"); 
        int fd = ::open(fname.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0666);
        ASSERT_GT(fd, 0) << strerror(errno);
        std::unique_ptr<int, std::function<void(int* fd)>> defer_close(&fd, [](int* fd) { ::close(*fd); });
    
        void* buf = nullptr;
        int ret = ::posix_memalign(&buf, buf_size, buf_size);
        std::unique_ptr<void, std::function<void(void*)>> auto_free(buf, [](void* p) { free(p); });
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

    for (auto& t : threads)
    {
        t.join();
    }
}
#endif
} // namespace tests
} // namespace DB

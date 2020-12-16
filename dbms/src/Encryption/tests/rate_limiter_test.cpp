#include <Encryption/RateLimiter.h>
#include <gtest/gtest.h>

#include <ctime>
#include <thread>

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
        auto rate_limiter = std::make_shared<RateLimiter>(nullptr, target, 100);
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
} // namespace tests
} // namespace DB

#include <Storages/BackgroundProcessingPool.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
void pauseAndDecreaseCounter(absl::BlockingCounter * counter, int * done)
{
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1000ms);
    *done = 1;
    counter->DecrementCount();
}

TEST(BlockingCounterTest, BasicFunctionality)
{
    // This test verifies that BlockingCounter functions correctly. Starts a
    // number of threads that just sleep for a second and decrement a counter.

    // Initialize the counter.
    const int num_workers = 10;
    absl::BlockingCounter counter(num_workers);

    std::vector<std::thread> workers;
    std::vector<int> done(num_workers, 0);

    // Start a number of parallel tasks that will just wait for a seconds and
    // then decrement the count.
    workers.reserve(num_workers);
    for (int k = 0; k < num_workers; k++)
    {
        workers.emplace_back(
            [&counter, &done, k] { pauseAndDecreaseCounter(&counter, &done[k]); });
    }

    // Wait for the threads to have all finished.
    counter.Wait();

    // Check that all the workers have completed.
    for (int k = 0; k < num_workers; k++)
    {
        EXPECT_EQ(1, done[k]);
    }

    for (auto & w : workers)
    {
        w.join();
    }
}

TEST(BlockingCounterTest, WaitZeroInitialCount)
{
    absl::BlockingCounter counter(0);
    counter.Wait();
}

} // namespace DB::tests

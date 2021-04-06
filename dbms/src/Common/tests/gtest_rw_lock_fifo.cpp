#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

#include <Common/RWLockFIFO.h>
#include <Common/Stopwatch.h>
#include <IO/WriteHelpers.h>
#include <common/ThreadPool.h>
#include <common/Types.h>

#include <atomic>
#include <iomanip>
#include <pcg_random.hpp>
#include <random>
#include <thread>


using namespace DB;


TEST(Common, RWLockFIFO_1)
{
    constexpr int cycles = 1000;
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static std::atomic<int> total_readers{0};
    static std::atomic<int> total_writers{0};

    static auto fifo_lock = RWLockFIFO::create();

    auto func = [&](size_t threads, int round) {
        std::random_device rd;
        pcg64 gen(rd());
        std::uniform_int_distribution<> d(0, 9);

        for (int i = 0; i < cycles; ++i)
        {
            auto r = d(gen);
            auto type = (r >= round) ? RWLockFIFO::Read : RWLockFIFO::Write;
            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));

            auto lock = fifo_lock->getLock(type, "RW");

            if (type == RWLockFIFO::Write)
            {
                ++writers;

                ASSERT_EQ(writers, 1);
                ASSERT_EQ(readers, 0);

                std::this_thread::sleep_for(sleep_for);

                ++total_writers;
                --writers;
            }
            else
            {
                ++readers;

                ASSERT_EQ(writers, 0);
                ASSERT_GE(readers, 1);
                ASSERT_LE(readers, threads);

                std::this_thread::sleep_for(sleep_for);

                ++total_readers;
                --readers;
            }
        }
    };

    for (auto pool_size : pool_sizes)
    {
        for (int round = 0; round < 10; ++round)
        {
            total_readers = 0;
            total_writers = 0;
            Stopwatch watch(CLOCK_MONOTONIC_COARSE);

            std::list<std::thread> threads;
            for (size_t thread = 0; thread < pool_size; ++thread)
                threads.emplace_back([=]() { func(pool_size, round); });

            for (auto & thread : threads)
                thread.join();

            auto total_time = watch.elapsedSeconds();
            std::cout << "Threads " << pool_size << ", round " << round << ", total_time " << DB::toString(total_time, 3)
                      << ", r:" << std::setw(4) << total_readers << ", w:" << std::setw(4) << total_writers << "\n";
        }
    }
}

TEST(Common, RWLockFIFO_Recursive)
{
    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLockFIFO::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&]() {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLockFIFO::Write);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&]() {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLockFIFO::Read);

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLockFIFO::Read);

            EXPECT_ANY_THROW({ fifo_lock->getLock(RWLockFIFO::Write); });
        }

        fifo_lock->getLock(RWLockFIFO::Write);
    });

    t1.join();
    t2.join();
}


TEST(Common, RWLockFIFO_PerfTest_Readers)
{
    constexpr int cycles = 100000; // 100k
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static auto fifo_lock = RWLockFIFO::create();

    for (auto pool_size : pool_sizes)
    {
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        auto func = [&]() {
            for (auto i = 0; i < cycles; ++i)
            {
                auto lock = fifo_lock->getLock(RWLockFIFO::Read);
            }
        };

        std::list<std::thread> threads;
        for (size_t thread = 0; thread < pool_size; ++thread)
            threads.emplace_back(func);

        for (auto & thread : threads)
            thread.join();

        auto total_time = watch.elapsedSeconds();
        std::cout << "Threads " << pool_size << ", total_time " << std::setprecision(2) << total_time << "\n";
    }

    for (auto pool_size : pool_sizes)
    {
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        auto func = [&]() {
            for (auto i = 0; i < cycles; ++i)
            {
                auto lock = fifo_lock->getLock(RWLockFIFO::Read, "test_query_id");
            }
        };

        std::list<std::thread> threads;
        for (size_t thread = 0; thread < pool_size; ++thread)
            threads.emplace_back(func);

        for (auto & thread : threads)
            thread.join();

        auto total_time = watch.elapsedSeconds();
        std::cout << "Threads " << pool_size << ", total_time " << std::setprecision(2) << total_time << "\n";
    }
}

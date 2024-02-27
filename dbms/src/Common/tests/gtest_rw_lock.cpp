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

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <gtest/gtest.h>

#include <atomic>
#include <iomanip>
#include <pcg_random.hpp>
#include <random>
#include <thread>


using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
}
} // namespace DB


TEST(Common, RWLock1)
{
    constexpr int cycles = 1000;
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static std::atomic<int> readers{0};
    static std::atomic<int> writers{0};

    static std::atomic<int> total_readers{0};
    static std::atomic<int> total_writers{0};

    static auto fifo_lock = RWLock::create();

    auto func = [&](size_t threads, int round) {
        std::random_device rd;
        pcg64 gen(rd());
        std::uniform_int_distribution<> d(0, 9);

        for (int i = 0; i < cycles; ++i)
        {
            auto r = d(gen);
            auto type = (r >= round) ? RWLock::Read : RWLock::Write;
            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));

            auto lock = fifo_lock->getLock(type, RWLock::NO_QUERY);

            if (type == RWLock::Write)
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
                ASSERT_LE((size_t)readers.load(), threads);

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
            std::cout << "Threads " << pool_size << ", round " << round << ", total_time "
                      << DB::toString(total_time, 3) << ", r:" << std::setw(4) << total_readers
                      << ", w:" << std::setw(4) << total_writers << "\n";
        }
    }
}

TEST(Common, RWLockRecursive)
{
    constexpr auto cycles = 10000;

    static auto fifo_lock = RWLock::create();

    static thread_local std::random_device rd;
    static thread_local pcg64 gen(rd());

    std::thread t1([&]() {
        for (int i = 0; i < 2 * cycles; ++i)
        {
            auto lock = fifo_lock->getLock(RWLock::Write, "q1");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);
        }
    });

    std::thread t2([&]() {
        for (int i = 0; i < cycles; ++i)
        {
            auto lock1 = fifo_lock->getLock(RWLock::Read, "q2");

            auto sleep_for = std::chrono::duration<int, std::micro>(std::uniform_int_distribution<>(1, 100)(gen));
            std::this_thread::sleep_for(sleep_for);

            auto lock2 = fifo_lock->getLock(RWLock::Read, "q2");

#ifndef ABORT_ON_LOGICAL_ERROR
            /// It throws LOGICAL_ERROR
            EXPECT_ANY_THROW({ fifo_lock->getLock(RWLock::Write, "q2"); });
#endif
        }

        fifo_lock->getLock(RWLock::Write, "q2");
    });

    t1.join();
    t2.join();
}


TEST(Common, RWLockDeadlock)
{
    static auto lock1 = RWLock::create();
    static auto lock2 = RWLock::create();

    /**
      * q1: r1          r2
      * q2:    w1
      * q3:       r2       r1
      * q4:          w2
      */

    std::thread t1([&]() {
        auto holder1 = lock1->getLock(RWLock::Read, "q1");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder2 = lock2->getLock(RWLock::Read, "q1", std::chrono::milliseconds(100));
            if (!holder2)
            {
                throw Exception(
                    "Locking attempt timed out! Possible deadlock avoided. Client should retry.",
                    ErrorCodes::DEADLOCK_AVOIDED);
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t2([&]() {
        usleep(100000);
        auto holder1 = lock1->getLock(RWLock::Write, "q2");
    });

    std::thread t3([&]() {
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLock::Read, "q3");
        usleep(100000);
        usleep(100000);
        usleep(100000);
        try
        {
            auto holder1 = lock1->getLock(RWLock::Read, "q3", std::chrono::milliseconds(100));
            if (!holder1)
            {
                throw Exception(
                    "Locking attempt timed out! Possible deadlock avoided. Client should retry.",
                    ErrorCodes::DEADLOCK_AVOIDED);
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DEADLOCK_AVOIDED)
                throw;
        }
    });

    std::thread t4([&]() {
        usleep(100000);
        usleep(100000);
        usleep(100000);
        auto holder2 = lock2->getLock(RWLock::Write, "q4");
    });

    t1.join();
    t2.join();
    t3.join();
    t4.join();
}


TEST(Common, RWLockPerfTestReaders)
{
    constexpr int cycles = 100000; // 100k
    const std::vector<size_t> pool_sizes{1, 2, 4, 8};

    static auto fifo_lock = RWLock::create();

    for (auto pool_size : pool_sizes)
    {
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        auto func = [&]() {
            for (auto i = 0; i < cycles; ++i)
            {
                auto lock = fifo_lock->getLock(RWLock::Read, RWLock::NO_QUERY);
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
                auto lock = fifo_lock->getLock(RWLock::Read, "test_query_id");
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

// Copyright 2022 PingCAP, Ltd.
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

#include <Common/FiberRWLock.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace DB::tests
{
namespace
{
struct FiberTypes
{
    using VoidPromise = boost::fibers::promise<void>;
    using VoidSharedFuture = boost::fibers::shared_future<void>;
    using Thread = boost::fibers::fiber;
};

struct ThreadTypes
{
    using VoidPromise = std::promise<void>;
    using VoidSharedFuture = std::shared_future<void>;
    using Thread = std::thread;
};

class FiberRWLockTest : public ::testing::Test
{
protected:
    template <typename Traits>
    void testLock()
    {
        FiberRWLock rw_lock;
        typename Traits::VoidPromise t1_stop;
        typename Traits::VoidPromise t2_stopped;
        typename Traits::VoidPromise t1_entered;
        typename Traits::VoidSharedFuture t1_entered_future(t1_entered.get_future());

        typename Traits::Thread t1([&] {
            auto future = t1_stop.get_future();
            std::unique_lock lock(rw_lock);
            t1_entered.set_value();
            future.get();
        });

        std::atomic_bool t2_entered = false;
        typename Traits::Thread t2([&] {
            t1_entered_future.get();
            std::unique_lock lock(rw_lock);
            t2_entered.store(true);
            t2_stopped.set_value();
        });

        t1_entered_future.get();
        ASSERT_EQ(t2_entered.load(), false);

        t1_stop.set_value();
        t2_stopped.get_future().get();
        ASSERT_EQ(t2_entered.load(), true);

        t1.join();
        t2.join();
    }

    template <typename Traits>
    void testTryLock()
    {
        FiberRWLock rw_lock;
        typename Traits::VoidPromise t1_stop;
        typename Traits::VoidPromise t2_stopped;
        typename Traits::VoidPromise t1_entered;
        typename Traits::VoidSharedFuture t1_entered_future(t1_entered.get_future());

        typename Traits::Thread t1([&] {
            auto future = t1_stop.get_future();
            std::unique_lock lock(rw_lock);
            t1_entered.set_value();
            future.get();
        });

        std::atomic_bool t2_locked = false;
        typename Traits::Thread t2([&] {
            t1_entered_future.get();
            t2_locked = rw_lock.try_lock();
            if (t2_locked)
                rw_lock.unlock();
            t2_stopped.set_value();
        });

        t1_entered_future.get();
        t2_stopped.get_future().get();
        ASSERT_EQ(t2_locked.load(), false);

        t1_stop.set_value();
        t1.join();
        t2.join();
    }

    template <typename Traits>
    void testLockShared()
    {
        FiberRWLock rw_lock;
        typename Traits::VoidPromise t1_stop;
        typename Traits::VoidPromise t2_stopped;
        typename Traits::VoidPromise t1_entered;
        typename Traits::VoidSharedFuture t1_entered_future(t1_entered.get_future());

        typename Traits::Thread t1([&] {
            auto future = t1_stop.get_future();
            std::shared_lock lock(rw_lock);
            t1_entered.set_value();
            future.get();
        });

        std::atomic_bool t2_entered = false;
        typename Traits::Thread t2([&] {
            t1_entered_future.get();
            std::shared_lock lock(rw_lock);
            t2_entered.store(true);
            t2_stopped.set_value();
        });

        t1_entered_future.get();
        t2_stopped.get_future().get();
        ASSERT_EQ(t2_entered.load(), true);
        t1_stop.set_value();

        t1.join();
        t2.join();
    }

};

TEST_F(FiberRWLockTest, testLockThread)
try
{
    testLock<ThreadTypes>();
}
CATCH

TEST_F(FiberRWLockTest, testLockFiber)
try
{
    testLock<FiberTypes>();
}
CATCH

TEST_F(FiberRWLockTest, testUniqueTryLockThread)
try
{
    testTryLock<ThreadTypes>();
}
CATCH

TEST_F(FiberRWLockTest, testUniqueTryLockFiber)
try
{
    testTryLock<FiberTypes>();
}
CATCH

TEST_F(FiberRWLockTest, testLockSharedThread)
try
{
    testLockShared<ThreadTypes>();
}
CATCH

TEST_F(FiberRWLockTest, testLockSharedFiber)
try
{
    testLockShared<FiberTypes>();
}
CATCH
} // namespace
} // namespace DB::tests


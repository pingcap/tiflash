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
#include <thread>

namespace DB::tests
{
namespace
{
class FiberRWLockTest : public ::testing::Test
{
};

TEST_F(FiberRWLockTest, testUniqueLockThread)
try
{
    FiberRWLock rw_lock;
    std::promise<void> t1_stop;
    std::promise<void> t2_stopped;
    std::promise<void> t1_entered;
    std::shared_future<void> t1_entered_future(t1_entered.get_future());

    std::thread t1([&] {
        auto future = t1_stop.get_future();
        std::unique_lock lock(rw_lock);
        t1_entered.set_value();
        t1_stop.get();
    });

    std::atomic_bool t2_entered = false;
    std::thread t2([&] {
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
}
CATCH

} // namespace
} // namespace DB::tests


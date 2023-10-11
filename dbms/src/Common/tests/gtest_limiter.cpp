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

#include <Common/Limiter.h>
#include <gtest/gtest.h>
#include <Common/ThreadManager.h>
#include <atomic>
#include <thread>

namespace DB::tests
{
class LimiterTest : public ::testing::Test
{
};

TEST_F(LimiterTest, timeout)
{
    Limiter<int> limiter{1};
    auto thread_mgr = newThreadManager();
    const std::chrono::milliseconds timeout(10);

    {
        auto ret = limiter.executeFor([] { return 1; }, timeout, [] { return 2; });
        ASSERT_EQ(ret, 1);
    }

    std::atomic_bool is_stop = false;
    thread_mgr->schedule(false, "test", [&] {
        auto ret = limiter.execute([&] {
            while (!is_stop)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            return 0;
        });
        ASSERT_EQ(ret, 0);
    });

    {
        auto ret = limiter.executeFor([] { return 1; }, timeout, [] { return 2; });
        ASSERT_EQ(ret, 2);
    }

    is_stop = true;
    thread_mgr->wait();
}
} // namespace DB::tests

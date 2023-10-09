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

#include <Common/SimpleFixedThreadPool.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <thread>
#include "Common/DynamicThreadPool.h"

namespace DB::tests
{
namespace
{
class SimpleFixedThreadPoolTest : public ::testing::Test
{
};

TEST_F(SimpleFixedThreadPoolTest, basic)
try
{
    // case1
    {
        SimpleFixedThreadPool pool("test", 2);
        ASSERT_EQ(pool.size(), 2);
        ASSERT_EQ(pool.active(), 0);
        std::atomic<int> a = 0;
        for (size_t i = 0; i < 10 ; ++i)
        {
            ASSERT_LE(pool.active(), pool.size());
            pool.schedule([&]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                ++a;
            });
        }
        ASSERT_LE(pool.active(), pool.size());
        while (a != 10)
        {
            ASSERT_LE(pool.active(), pool.size());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(pool.active(), 0);
    }

    // case2
    {
        SimpleFixedThreadPool pool("test", 2);
        ASSERT_EQ(pool.size(), 2);
        ASSERT_EQ(pool.active(), 0);
        std::atomic<bool> stop = false;
        for (size_t i = 0; i < pool.size(); ++i)
        {
            pool.schedule([&]() {
                while (true)
                {
                    if (stop)
                        return;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });
            ASSERT_EQ(pool.active(), i + 1);
        }
        stop = true;
    }
}
CATCH

} // namespace
} // namespace DB::tests

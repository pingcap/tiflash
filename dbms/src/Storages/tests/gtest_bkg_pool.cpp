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

#include <Common/Logger.h>
#include <Storages/BackgroundProcessingPool.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <thread>

namespace DB::tests
{

TEST(BackgroundProcessingPoolTest, FixedInterval)
{
    BackgroundProcessingPool pool(10, "test", std::make_shared<JointThreadInfoJeallocMap>());

    using Clock = std::chrono::system_clock;
    using TimePoint = std::chrono::time_point<Clock>;


    using namespace std::chrono_literals;
    const auto sleep_seconds = 10s;
    const Int64 expect_interval_ms = 2 * 1000;
    const auto num_expect_called = 5;

    Int64 num_actual_called = 0;
    TimePoint last_update_timepoint = Clock::now();
    Int64 min_diff_ms = std::numeric_limits<Int64>::max();
    Int64 max_diff_ms = 0;
    auto task = pool.addTask(
        [&]() {
            num_actual_called += 1;
            if (num_actual_called != 1)
            {
                auto diff_ms
                    = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - last_update_timepoint)
                          .count();
                if (diff_ms < expect_interval_ms / 2)
                {
                    LOG_ERROR(Logger::get(), "Unexpected frequent call, actual interval={}ms", diff_ms);
                }
                min_diff_ms = std::min(min_diff_ms, diff_ms);
                max_diff_ms = std::max(max_diff_ms, diff_ms);
            }

            last_update_timepoint = Clock::now();
            return false; // expected to be run n a fixed interval
        },
        /*multi*/ false,
        expect_interval_ms);

    std::this_thread::sleep_for(sleep_seconds);

    pool.removeTask(task);

    LOG_INFO(
        Logger::get(),
        "actual being called for {} times, min_diff={} max_diff={}",
        num_actual_called,
        min_diff_ms,
        max_diff_ms);
    ASSERT_TRUE(num_expect_called - 1 <= num_actual_called && num_actual_called <= num_expect_called + 1)
        << fmt::format("actual_called={} min_diff_ms={}", num_actual_called, min_diff_ms);
}

} // namespace DB::tests

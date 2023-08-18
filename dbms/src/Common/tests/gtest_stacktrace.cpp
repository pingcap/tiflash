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

#include <Common/StackTrace.h>
#include <common/defines.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
namespace DB
{
namespace tests
{
NO_INLINE void function_0(bool output = false)
{
    auto res = StackTrace().toString();
    std::string::size_type idx;
    if (output)
    {
        std::cout << res << std::endl;
    }
    idx = res.find("function_0");
    EXPECT_NE(idx, std::string::npos);
    idx = res.find("function_1", idx);
    EXPECT_NE(idx, std::string::npos);
    idx = res.find("function_2", idx);
    EXPECT_NE(idx, std::string::npos);
}
NO_INLINE void function_1(bool output = false, size_t level = 0)
{
    if (level == 0)
    {
        function_0(output);
    }
    else
    {
        function_1(output, level - 1);
    }
}
NO_INLINE void function_2(bool output = false, size_t level = 0)
{
    function_1(output, level);
}

// Sanitizers wrongly report info on the rust side and they may mess up the stacktrace.
// Setting no_sanitize does not fix the issue.
// we skip the stacktrace tests for TSAN
#if !defined(THREAD_SANITIZER) && !defined(ADDRESS_SANITIZER)
TEST(StackTrace, SingleThread)
{
    function_2(true);
    function_2(true, 16);
}
TEST(StackTrace, MultiThreads)
{
    size_t num = std::thread::hardware_concurrency();
    std::vector<std::thread> threads{};
    for (size_t i = 0; i < num; ++i)
    {
        threads.emplace_back([] {
            for (int j = 0; j < 16; ++j)
            {
                function_2(false, j);
            }
        });
    }
    for (auto & i : threads)
    {
        i.join();
    }
}
#endif

} // namespace tests
} // namespace DB

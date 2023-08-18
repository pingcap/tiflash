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
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(ExceptionTest, RuntimeCheck)
{
    auto a = 1;
    auto b = 2;

    RUNTIME_CHECK(a != b);

    EXPECT_THROW(
        {
            try
            {
                RUNTIME_CHECK(a == b);
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ("Check a == b failed", e.message());
                throw;
            }
        },
        DB::Exception);

    EXPECT_THROW(
        {
            try
            {
                RUNTIME_CHECK(a == b, a, b);
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ("Check a == b failed, a = 1, b = 2", e.message());
                throw;
            }
        },
        DB::Exception);

    EXPECT_THROW(
        {
            try
            {
                RUNTIME_CHECK(a == b, a + b);
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ("Check a == b failed, a + b = 3", e.message());
                throw;
            }
        },
        DB::Exception);
}

TEST(ExceptionTest, RuntimeCheckMsg)
{
    auto a = 1;
    auto b = 2;

    RUNTIME_CHECK_MSG(a != b, "Check failed");

    EXPECT_THROW(
        {
            try
            {
                RUNTIME_CHECK_MSG(a == b, "Invalid input");
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ("Check a == b failed: Invalid input", e.message());
                throw;
            }
        },
        DB::Exception);

    EXPECT_THROW(
        {
            try
            {
                RUNTIME_CHECK_MSG(a == b, "Invalid input, a = {}", a);
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ("Check a == b failed: Invalid input, a = 1", e.message());
                throw;
            }
        },
        DB::Exception);
}

} // namespace DB::tests

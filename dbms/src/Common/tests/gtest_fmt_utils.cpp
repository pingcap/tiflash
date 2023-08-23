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

#include <Common/FmtUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{
TEST(FmtUtilsTest, TestFmtBuffer)
{
    FmtBuffer buffer;
    buffer.append("{").append("test").append("}");
    ASSERT_EQ(buffer.toString(), "{test}");

    buffer.fmtAppend(" fmt append {}", "test");
    ASSERT_EQ(buffer.toString(), "{test} fmt append test");
}

TEST(FmtUtilsTest, TestJoinStr)
{
    FmtBuffer buffer;
    std::vector<std::string> v{"a", "b", "c"};
    buffer.joinStr(v.cbegin(), v.cend());
    ASSERT_EQ(buffer.toString(), "a, b, c");

    buffer.clear();
    v.clear();
    buffer.joinStr(v.cbegin(), v.cend());
    ASSERT_EQ(buffer.toString(), "");

    buffer.clear();
    v.push_back("a");
    buffer.joinStr(v.cbegin(), v.cend())
        .joinStr(
            v.cbegin(),
            v.cend(),
            [](const auto & s, FmtBuffer & fb) {
                fb.append(s);
                fb.append("t");
            },
            ", ");
    ASSERT_EQ(buffer.toString(), "aat");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+");
    ASSERT_EQ(buffer.toString(), "a+b");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+").joinStr(v.cbegin(), v.cend(), "-");
    ASSERT_EQ(buffer.toString(), "a+ba-b");
}
} // namespace DB::tests
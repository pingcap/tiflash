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
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>

namespace DB
{
namespace tests
{

TEST(WriteBufferFromOwnString, TestFinalizeShortBuffer)
{
    WriteBufferFromOwnString buffer;
    buffer << "a";

    std::string str = buffer.str();
    EXPECT_EQ(str, "a");
    EXPECT_EQ(buffer.count(), str.size());
}

TEST(WriteBufferFromOwnString, TestFinalizeLongBuffer)
{
    std::string expect;
    WriteBufferFromOwnString buffer;

    /// 100 is long enough to trigger next
    for (size_t i = 0; i < 100; ++i)
    {
        char c = 'a' + i % 26;
        expect.push_back(c);
        buffer.write(c);
    }

    std::string str = buffer.str();
    EXPECT_EQ(str, expect);
    EXPECT_EQ(buffer.count(), str.size());
}

} // namespace tests
} // namespace DB

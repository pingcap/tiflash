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

#include <Common/PODArray.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(Common, PODArrayInsert)
{
    std::string str = "test_string_abacaba";
    PODArray<char> chars;
    chars.insert(chars.end(), str.begin(), str.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_in_the_middle = "insert_in_the_middle";
    auto pos = str.size() / 2;
    str.insert(str.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    chars.insert(chars.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_with_resize;
    insert_with_resize.reserve(chars.capacity() * 2);
    char cur_char = 'a';
    while (insert_with_resize.size() < insert_with_resize.capacity())
    {
        insert_with_resize += cur_char;
        if (cur_char == 'z')
            cur_char = 'a';
        else
            ++cur_char;
    }
    str.insert(str.begin(), insert_with_resize.begin(), insert_with_resize.end());
    chars.insert(chars.begin(), insert_with_resize.begin(), insert_with_resize.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));
}

TEST(Common, PODPushBackRawMany)
{
    PODArray<char> chars;
    chars.push_back_raw_many(5, "first");
    EXPECT_EQ(std::string("first"), std::string(chars.data(), chars.size()));
    EXPECT_EQ(5UL, chars.size());
    EXPECT_LE(chars.capacity() - chars.size(), 10UL);
    chars.push_back_raw_many(10, "0123456789");
    EXPECT_EQ(15UL, chars.size());
    EXPECT_EQ(std::string("first0123456789"), std::string(chars.data(), chars.size()));
}

TEST(Common, PODNoOverallocation)
{
    /// Check that PaddedPODArray allocates for smaller number of elements than the power of two due to padding.
    /// NOTE: It's Ok to change these numbers if you will modify initial size or padding.

    PaddedPODArray<char> chars;
    std::vector<size_t> capacities;

    size_t prev_capacity = 0;
    for (size_t i = 0; i < 1000000; ++i)
    {
        chars.emplace_back();
        if (chars.capacity() != prev_capacity)
        {
            prev_capacity = chars.capacity();
            capacities.emplace_back(prev_capacity);
        }
    }

    EXPECT_EQ(capacities, (std::vector<size_t>{4017, 8113, 16305, 32689, 65457, 130993, 262065, 524209, 1048497}));
}

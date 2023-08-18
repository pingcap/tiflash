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

#include <common/defines.h>
#include <common/mem_utils_opt.h>
#include <common/memcpy.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <string_view>
#include <utility>

#if defined(TIFLASH_ENABLE_AVX_SUPPORT)

void TestFunc(size_t size)
{
    std::string oa(size + 2, '0');
    oa[size] = char(1);
    std::string ob = oa;
    ob[size] = char(2);

    std::string_view a{oa.data(), size};
    std::string_view b{ob.data(), size};
    ASSERT_TRUE(mem_utils::IsStrViewEqual(a, b));
    ASSERT_EQ(nullptr, mem_utils::avx2_memchr(a.data(), size, '1'));

    for (size_t first_fail_pos = 0; first_fail_pos < size; ++first_fail_pos)
    {
        auto tmp = ob[first_fail_pos];
        ob[first_fail_pos] = '1';
        ASSERT_EQ(first_fail_pos, mem_utils::StrFind(b, "1"));
        ASSERT_FALSE(mem_utils::IsStrViewEqual(a, b));
        ASSERT_TRUE(mem_utils::IsStrViewEqual({a.data(), first_fail_pos}, {b.data(), first_fail_pos}));
        ob[first_fail_pos] = tmp;
    }
}

void TestStrCmpFunc(size_t size)
{
    std::string oa(size + 2, '1');
    oa[size] = char(1);
    std::string ob = oa;
    ob[size] = char(2);

    std::string_view a{oa.data(), size};
    std::string_view b{ob.data(), size};

    ASSERT_EQ(mem_utils::CompareStrView(a, b), 0);

    for (size_t first_fail_pos = 0; first_fail_pos < size; ++first_fail_pos)
    {
        auto tmp = ob[first_fail_pos];
        ob[first_fail_pos] = '2';
        ASSERT_LT(mem_utils::CompareStrView(a, b), 0);
        ASSERT_GT(mem_utils::CompareStrView(b, a), 0);
        ob[first_fail_pos] = '0';
        ASSERT_GT(mem_utils::CompareStrView(a, b), 0);
        ASSERT_LT(mem_utils::CompareStrView(b, a), 0);
        ob[first_fail_pos] = tmp;
    }
}

TEST(MemUtilsTestOPT, CompareNormal)
{
    // size not equal
    ASSERT_FALSE(mem_utils::IsStrViewEqual("123", "1"));

    for (size_t size = 0; size < (256 + 128 + 10); ++size)
    {
        TestFunc(size);
    }
    {
        std::string_view a = "12";
        std::string_view b = "1234";
        ASSERT_EQ(mem_utils::StrFind(a, b), -1);
        ASSERT_EQ(*mem_utils::avx2_memchr(b.data(), b.size(), '4'), '4');

        a = "";
        b = "";
        ASSERT_EQ(mem_utils::StrFind(a, b), 0);
    }
    for (size_t size = 0; size < 256; ++size)
    {
        std::string a(size + 50, char(0));
        auto * start
            = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned

        for (size_t first_pos = 0; first_pos < size; ++first_pos)
        {
            for (size_t needle_size = 1; needle_size + first_pos <= size; ++needle_size)
            {
                std::memset(start + first_pos, -1, needle_size);
                {
                    ASSERT_EQ(mem_utils::StrFind({start, size}, {start + first_pos, needle_size}), first_pos);
                }
                std::memset(start + first_pos, 0, needle_size);
            }
        }
    }
    {
        size_t size = 10;
        std::string a(size + 50, char(0));
        auto * start
            = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[-5] = 1;
        start[5] = 1;
        start[15] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1, mem_utils::StrFind({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 6;
        std::string a(size + 50, char(0));
        auto * start
            = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[-5] = 1;
        start[23] = 1;
        start[29] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1, mem_utils::StrFind({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 32 + 5;
        std::string a(size + 50, char(0));
        auto * start
            = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[23] = 1;
        start[23 + 4] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1, mem_utils::StrFind({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 32 * 5 + 5;
        std::string a(size + 50, char(0));
        auto * start
            = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[22 + 2 * 32] = 1;
        start[22 + 2 * 32 + 6] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1, mem_utils::StrFind({start, size}, b));
        ASSERT_EQ(-1, mem_utils::avx2_strstr(start, size, b.data(), b.size()));
    }
    {
        std::string a(32, char(0));
        char * p = a.data() + 16 - size_t(a.data()) % 16 + 5;
        ASSERT_EQ(nullptr, mem_utils::avx2_memchr(p, 5, char(1)));
    }
}

TEST(MemUtilsTestOPT, CompareStr)
{
    // size not equal
    ASSERT_EQ(mem_utils::CompareStrView("123", "1"), 1);
    ASSERT_EQ(mem_utils::CompareStrView("123", "123"), 0);
    ASSERT_EQ(mem_utils::CompareStrView("123", "1234"), -1);
    ASSERT_EQ(mem_utils::CompareStrView("1", ""), 1);
    ASSERT_EQ(mem_utils::CompareStrView("", ""), 0);
    ASSERT_EQ(mem_utils::CompareStrView("", "1"), -1);

    for (size_t size = 0; size < (256 + 128 + 10); ++size)
    {
        TestStrCmpFunc(size);
    }
}

template <ssize_t overlap_offset, typename F>
void TestMemCopyFunc(size_t size, F && fn_memcpy)
{
    std::string oa(size + 100, 0);
    char * start = oa.data();
    start += (16 - size_t(start) % 16);
    start += 5;
    {
        uint8_t n1 = 1, n2 = 2;
        for (auto * p = start; p != start + size; ++p)
        {
            *p = n1 + n2;
            n1 = n2;
            n2 = *p;
        }
    }

    std::string ob;
    char * tar{};

    if constexpr (overlap_offset)
        tar = start + overlap_offset;
    else
    {
        ob.resize(size + 100, 0);
        tar = ob.data();
        tar += (16 - size_t(tar) % 16);
        tar += 1;
    }

    fn_memcpy(tar, start, size);
    {
        uint8_t n1 = 1, n2 = 2;
        for (const auto * p = tar; p != tar + size; ++p)
        {
            ASSERT_EQ(uint8_t(*p), uint8_t(n1 + n2));
            n1 = n2;
            n2 = *p;
        }
    }
}

TEST(MemUtilsTestOPT, Memcopy)
{
    for (size_t size = 0; size < 600; ++size)
    {
        TestMemCopyFunc<0>(size, mem_utils::avx2_inline_memcpy);
        TestMemCopyFunc<0>(size, sse2_inline_memcpy);
    }
}

void TestMemByteCount(size_t size)
{
    char target = 8;
    std::string oa(size + 100, target);
    char * start = oa.data();
    for (auto * pos = start; pos < start + 32; ++pos)
    {
        ASSERT_EQ(mem_utils::avx2_byte_count(pos, size, target), size);
        std::memset(pos, target - 1, size);
        ASSERT_EQ(mem_utils::avx2_byte_count(pos, size, target), 0);
        std::memset(pos, target, size);
    }
}

TEST(MemUtilsTestOPT, MemByteCount)
{
    for (size_t size = 0; size <= 32 * 6; ++size)
    {
        TestMemByteCount(size);
    }
}

#endif
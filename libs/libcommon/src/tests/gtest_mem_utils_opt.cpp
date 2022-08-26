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

#include <common/defines.h>
#include <common/mem_utils_opt.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <random>
#include <string_view>
#include <utility>


FLATTEN_INLINE_PURE static inline bool IsRawStrEqual(const std::string_view & lhs, const std::string_view & rhs)
{
    if (lhs.size() != rhs.size())
        return false;

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    return mem_utils::avx2_mem_equal(lhs.data(), rhs.data(), lhs.size());
#else
    return 0 == std::memcmp(lhs.data(), rhs.data(), lhs.size());
#endif
}

#if defined(TIFLASH_ENABLE_AVX_SUPPORT)

void BaseTestFunc(size_t size,
                  size_t first_fail_pos)
{
    std::string a(size, '0');
    std::string b = a;
    ASSERT_TRUE(IsRawStrEqual(a, b));
    ASSERT_EQ(nullptr, mem_utils::avx2_memchr(a.data(), size, '1'));

    if (size == first_fail_pos)
        return;
    b[first_fail_pos] = '1';
    ASSERT_FALSE(IsRawStrEqual(a, b));
    ASSERT_EQ(first_fail_pos, mem_utils::avx2_strstr(b, "1"));
}

TEST(MemUtilsTestOPT, CompareNormal)
{
    // size not equal
    ASSERT_FALSE(IsRawStrEqual("123", "1"));

    for (size_t size = 0; size < (256 + 128 + 10); ++size)
    {
        for (size_t first_fail_pos = 0; first_fail_pos <= size; ++first_fail_pos)
        {
            BaseTestFunc(size, first_fail_pos);
        }
    }
    {
        std::string_view a = "12";
        std::string_view b = "1234";
        ASSERT_EQ(mem_utils::avx2_strstr(a, b), -1);
        a = "";
        b = "";
        ASSERT_EQ(mem_utils::avx2_strstr(a, b), 0);
    }
    for (size_t size = 0; size < 256; ++size)
    {
        std::string a(size + 50, char(0));
        auto * start = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned

        for (size_t first_pos = 0; first_pos < size; ++first_pos)
        {
            for (size_t needle_size = 1; needle_size + first_pos <= size; ++needle_size)
            {
                std::memset(start + first_pos, -1, needle_size);
                {
                    ASSERT_EQ(mem_utils::avx2_strstr(start, size, start + first_pos, needle_size), first_pos);
                }
                std::memset(start + first_pos, 0, needle_size);
            }
        }
    }
    {
        size_t size = 10;
        std::string a(size + 50, char(0));
        auto * start = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[-5] = 1;
        start[5] = 1;
        start[15] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1,
                  mem_utils::avx2_strstr({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 6;
        std::string a(size + 50, char(0));
        auto * start = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[-5] = 1;
        start[23] = 1;
        start[29] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1,
                  mem_utils::avx2_strstr({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 32 + 5;
        std::string a(size + 50, char(0));
        auto * start = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[23] = 1;
        start[23 + 4] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1,
                  mem_utils::avx2_strstr({start, size}, b));
    }
    {
        size_t size = 32 - 10 + 32 * 5 + 5;
        std::string a(size + 50, char(0));
        auto * start = reinterpret_cast<char *>((size_t(a.data()) + 32 - 1) / 32 * 32 + 10); // start address not aligned
        start[22 + 2 * 32] = 1;
        start[22 + 2 * 32 + 6] = 1;
        std::string b(2, char(1));
        ASSERT_EQ(-1,
                  mem_utils::avx2_strstr({start, size}, b));
    }
}

#endif

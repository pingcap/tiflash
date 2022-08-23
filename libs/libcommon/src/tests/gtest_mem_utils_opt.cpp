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

#include <random>
#include <utility>

struct AlignedCharArray
{
    std::align_val_t alignment;
    char * data;

    AlignedCharArray(size_t size_, std::align_val_t alignment_)
        : alignment(alignment_)
        , data(static_cast<char *>(operator new(size_, alignment)))
    {}

    ~AlignedCharArray() { ::operator delete(data, alignment); }
};

FLATTEN_INLINE_PURE static inline bool IsRawStrEqual(const std::string_view & lhs, const std::string_view & rhs)
{
    if (lhs.size() != rhs.size())
        return false;

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#ifdef TIFLASH_USE_AVX2_COMPILE_FLAG
    return mem_utils::avx2_mem_equal(lhs.data(), rhs.data(), lhs.size());
#else
    return avx2_mem_equal(lhs.data(), rhs.data(), lhs.size());
#endif
#else
    return 0 == std::memcmp(lhs.data(), rhs.data(), lhs.size());
#endif
}

#if defined(TIFLASH_ENABLE_AVX_SUPPORT)

TEST(MemUtilsTestOPT, CompareTrivial)
{
    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123", "123"},
             {"abc", "abc"},
             {"\v\a\t\n213@3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '@')}})
    {
        ASSERT_TRUE(IsRawStrEqual(std::string_view(a), std::string_view(b)));
    }

    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123-", "-123"},
             {"ab", "abc"},
             {"\a\t\n213#3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '!')}})
    {
        ASSERT_FALSE(IsRawStrEqual(std::string_view(a), std::string_view(b)));
    }
}


TEST(MemUtilsTestOPT, CompareLongEq)
{
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto tmp_aligned1 = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto tmp_aligned2 = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto * aligned1 = tmp_aligned1.data;
    auto * aligned2 = tmp_aligned2.data;
    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    ASSERT_TRUE(IsRawStrEqual(std::string_view(aligned1, data.size()), std::string_view(aligned2, data.size()))) << " seed: " << seed;
}

TEST(MemUtilsTestOPT, CompareLongNe)
{
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto tmp_aligned1 = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto tmp_aligned2 = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto * aligned1 = tmp_aligned1.data;
    auto * aligned2 = tmp_aligned2.data;

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    auto target = eng() % data.size();
    aligned2[target] = static_cast<char>(~aligned2[target]);

    ASSERT_FALSE(IsRawStrEqual(std::string_view(aligned1, data.size()), std::string_view(aligned2, data.size()))) << " seed: " << seed;
}

#endif

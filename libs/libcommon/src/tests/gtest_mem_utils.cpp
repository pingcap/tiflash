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

#include <common/StringRef.h>
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

#if defined(TIFLASH_ENABLE_AVX_SUPPORT) && defined(TIFLASH_ENABLE_AVX512_SUPPORT)

struct TempOption
{
    bool prev_enable_avx;
    bool prev_enable_avx512;

    TempOption(bool enable_avx, bool enable_avx512)
    {
        prev_enable_avx = simd_option::ENABLE_AVX;
        prev_enable_avx512 = simd_option::ENABLE_AVX512;
        simd_option::ENABLE_AVX = enable_avx;
        simd_option::ENABLE_AVX512 = enable_avx512;
    }

    ~TempOption()
    {
        simd_option::ENABLE_AVX = prev_enable_avx;
        simd_option::ENABLE_AVX512 = prev_enable_avx512;
    }
};

struct MemUtilsTest : ::testing::TestWithParam<std::pair<bool, bool>>
{
};
TEST_P(MemUtilsTest, CompareTrivial)
{
    TempOption option(GetParam().first, GetParam().second);
    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123", "123"},
             {"abc", "abc"},
             {"\v\a\t\n213@3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '@')}})
    {
        ASSERT_EQ(StringRef(a), StringRef(b));
    }

    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123-", "-123"},
             {"ab", "abc"},
             {"\a\t\n213#3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '!')}})
    {
        ASSERT_NE(StringRef(a), StringRef(b));
    }
}


TEST_P(MemUtilsTest, CompareLongEq)
{
    using namespace simd_option;
    TempOption option(GetParam().first, GetParam().second);
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<std::int8_t> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1_arr = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto aligned2_arr = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto * aligned1 = aligned1_arr.data;
    auto * aligned2 = aligned2_arr.data;
    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    ASSERT_EQ(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
}

TEST_P(MemUtilsTest, CompareLongNe)
{
    using namespace simd_option;
    TempOption option(GetParam().first, GetParam().second);
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<std::int8_t> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1_arr = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto aligned2_arr = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto * aligned1 = aligned1_arr.data;
    auto * aligned2 = aligned2_arr.data;

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    auto target = eng() % data.size();
    aligned2[target] = static_cast<char>(~aligned2[target]);

    ASSERT_NE(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
}

using Parm = std::pair<bool, bool>;
#define MAKE_PAIR(x, y) (std::make_pair(x, y))

std::string parmToName(const ::testing::TestParamInfo<Parm> & info)
{
    return fmt::format("avx_{}_avx512_{}", info.param.first, info.param.second);
}

INSTANTIATE_TEST_CASE_P(
    Parm,
    MemUtilsTest,
    testing::Values(MAKE_PAIR(false, false), MAKE_PAIR(false, true), MAKE_PAIR(true, false), MAKE_PAIR(true, true)),
    parmToName);

#endif

TEST(MemUtilsTest, MemoryIsZeroGeneric)
{
    auto length = 1024 * 128 - 3;
    auto memory = AlignedCharArray(length + 4, std::align_val_t{64});
    std::memset(memory.data, 0, length + 4);
    auto * ptr = memory.data + 1;
    ASSERT_TRUE(mem_utils::_detail::memoryIsByteGeneric(ptr, length, std::byte{0}));
    for (auto i = 0; i < length; ++i)
    {
        ptr[i] = 1;
        ASSERT_FALSE(mem_utils::_detail::memoryIsByteGeneric(ptr, length, std::byte{0}));
        ptr[i] = 0;
    }
}

#if defined(TIFLASH_ENABLE_AVX_SUPPORT)
TEST(MemUtilsTest, MemoryIsZeroAVX2)
{
    using namespace simd_option;
    if (!common::cpu_feature_flags.avx2)
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto length = 1024 * 128 - 3;
    auto memory = AlignedCharArray(length + 4, std::align_val_t{64});
    std::memset(memory.data, 0, length + 4);
    auto * ptr = memory.data + 1;
    ASSERT_TRUE(mem_utils::_detail::memoryIsByteAVX2(ptr, length, std::byte{0}));
    for (auto i = 0; i < length; ++i)
    {
        ptr[i] = 1;
        ASSERT_FALSE(mem_utils::_detail::memoryIsByteAVX2(ptr, length, std::byte{0}));
        ptr[i] = 0;
    }
}
#endif

#if defined(TIFLASH_ENABLE_AVX512_SUPPORT)
TEST(MemUtilsTest, MemoryIsZeroAVX512)
{
    using namespace simd_option;
    if (!common::cpu_feature_flags.avx512vl || !common::cpu_feature_flags.avx512bw)
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto length = 1024 * 128 - 3;
    auto memory = AlignedCharArray(length + 4, std::align_val_t{64});
    std::memset(memory.data, 0, length + 4);
    auto * ptr = memory.data + 1;
    ASSERT_TRUE(mem_utils::_detail::memoryIsByteAVX512(ptr, length, std::byte{0}));
    for (auto i = 0; i < length; ++i)
    {
        ptr[i] = 1;
        ASSERT_FALSE(mem_utils::_detail::memoryIsByteAVX512(ptr, length, std::byte{0}));
        ptr[i] = 0;
    }
}
#endif

#if __SSE2__
TEST(MemUtilsTest, MemoryIsZeroSSE2)
{
    auto length = 1024 * 128 - 3;
    auto memory = AlignedCharArray(length + 4, std::align_val_t{64});
    std::memset(memory.data, 0, length + 4);
    auto * ptr = memory.data + 1;
    ASSERT_TRUE(mem_utils::_detail::memoryIsByteSSE2(ptr, length, std::byte{0}));
    for (auto i = 0; i < length; ++i)
    {
        ptr[i] = 1;
        ASSERT_FALSE(mem_utils::_detail::memoryIsByteSSE2(ptr, length, std::byte{0}));
        ptr[i] = 0;
    }
}
#endif

#if defined(TIFLASH_ENABLE_ASIMD_SUPPORT)

TEST(MemUtilsTest, MemoryIsZeroASIMD)
{
    using namespace simd_option;
    if (!common::cpu_feature_flags.asimd)
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto length = 1024 * 128 - 3;
    auto memory = AlignedCharArray(length + 4, std::align_val_t{64});
    std::memset(memory.data, 0, length + 4);
    auto ptr = memory.data + 1;
    ASSERT_TRUE(mem_utils::_detail::memoryIsByteASIMD(ptr, length, std::byte{0}));
    for (auto i = 0; i < length; ++i)
    {
        ptr[i] = 1;
        ASSERT_FALSE(mem_utils::_detail::memoryIsByteASIMD(ptr, length, std::byte{0}));
        ptr[i] = 0;
    }
}

struct TempOption
{
    bool prev_enable_asimd;

    TempOption(bool enable_asimd)
    {
        prev_enable_asimd = simd_option::ENABLE_ASIMD;
        simd_option::ENABLE_ASIMD = enable_asimd;
    }

    ~TempOption() { simd_option::ENABLE_ASIMD = prev_enable_asimd; }
};
struct MemUtilsTest : ::testing::TestWithParam<bool>
{
};
TEST_P(MemUtilsTest, CompareTrivial)
{
    TempOption _option(GetParam());
    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123", "123"},
             {"abc", "abc"},
             {"\v\a\t\n213@3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '@')}})
    {
        ASSERT_EQ(StringRef(a), StringRef(b));
    }

    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123-", "-123"},
             {"ab", "abc"},
             {"\a\t\n213#3213", "\v\a\t\n213@3213"},
             {std::string(1024, '@'), std::string(1024, '!')}})
    {
        ASSERT_NE(StringRef(a), StringRef(b));
    }
}


TEST_P(MemUtilsTest, CompareLongEq)
{
    using namespace simd_option;
    TempOption _option(GetParam());
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<std::int8_t> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1_ = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto aligned2_ = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto aligned1 = aligned1_.data;
    auto aligned2 = aligned2_.data;

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    ASSERT_EQ(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
}

TEST_P(MemUtilsTest, CompareLongNe)
{
    using namespace simd_option;
    TempOption _option(GetParam());
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<int8_t> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1_ = AlignedCharArray(1024 * 1024 * 64 + 1, std::align_val_t{128});
    auto aligned2_ = AlignedCharArray(1024 * 1024 * 64 + 23 + 1, std::align_val_t{128});
    auto aligned1 = aligned1_.data;
    auto aligned2 = aligned2_.data;

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    auto target = eng() % data.size();
    aligned2[target] = static_cast<char>(~aligned2[target]);

    ASSERT_NE(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
}

std::string parmToName(const ::testing::TestParamInfo<bool> & info)
{
    if (info.param)
        return "asimd";
    return "generic";
}

INSTANTIATE_TEST_CASE_P(bool, MemUtilsTest, testing::Values(false, true), parmToName);

#endif

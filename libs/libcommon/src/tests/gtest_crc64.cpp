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

#include <common/crc64.h>
#include <common/simd.h>
#include <gtest/gtest.h>

#include <cstring>
#include <ext/scope_guard.h>
#include <iomanip>
#include <random>
#include <vector>
#if defined(__x86_64__)
#include <common/crc64_arch/crc64_x86.h>
#elif defined(__aarch64__) || defined(__arm64__)
#include <common/crc64_arch/crc64_aarch64.h>
#endif

using namespace crc64::_detail;
using namespace crc64;

template <class A, class B>
using Cases = std::vector<std::pair<A, B>>;


bool check_basic_support()
{
    using namespace common;
#if defined(__x86_64__)
    return cpu_feature_flags.pclmulqdq;
#elif defined(__aarch64__) || defined(__arm64__)
    return cpu_feature_flags.asimd && cpu_feature_flags.pmull;
#endif
}

struct FullFlagsGuard
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    bool prev_enable_avx;
#endif
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
    bool prev_enable_avx512;
#endif
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
    bool prev_enable_asimd;
#endif
    FullFlagsGuard()
    {
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
        prev_enable_avx = simd_option::ENABLE_AVX;
        simd_option::ENABLE_AVX = true;
#endif
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
        prev_enable_avx512 = simd_option::ENABLE_AVX512;
        simd_option::ENABLE_AVX512 = true;
#endif
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
        prev_enable_asimd = simd_option::ENABLE_ASIMD;
        simd_option::ENABLE_ASIMD = true;
#endif
    }
    ~FullFlagsGuard()
    {
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
        simd_option::ENABLE_AVX = prev_enable_avx;
#endif
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
        simd_option::ENABLE_AVX512 = prev_enable_avx512;
#endif
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
        simd_option::ENABLE_ASIMD = prev_enable_asimd;
#endif
    }
};

TEST(CRC64, SizeAlign)
{
    ASSERT_EQ(alignof(SIMD), 16ull);
    ASSERT_EQ(sizeof(SIMD), 16ull);
}

TEST(CRC64, Equality)
{
    if (!check_basic_support())
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto x = SIMD{0xd7c8'11cf'e5c5'c792, 0x86e6'5c36'e68b'4804};
    auto y = SIMD{0xd7c8'11cf'e5c5'c792, 0x86e6'5c36'e68b'4804};
    auto z = SIMD{0xfa3e'0099'cd5e'd60d, 0xad71'9ee6'57d1'498e};
    ASSERT_EQ(x, y);
    ASSERT_FALSE(x == z);
}

TEST(CRC64, Xor)
{
    if (!check_basic_support())
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto x = SIMD{0xe450'87f9'b031'0d47, 0x3d72'e92a'96c7'4c63};
    auto y = SIMD{0x7ed8'ae0a'dfbd'89c0, 0x1c9b'dfaa'953e'0ef4};
    auto z = x ^ y;
    ASSERT_EQ(z, SIMD(0x9a88'29f3'6f8c'8487, 0x21e9'3680'03f9'4297));
    z ^= SIMD{0x57a2'0f44'c005'b2ea, 0x7056'bde9'9303'aa51};
    ASSERT_EQ(z, SIMD(0xcd2a'26b7'af89'366d, 0x51bf'8b69'90fa'e8c6));
}

TEST(CRC64, Fold16)
{
    if (!check_basic_support())
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto x = SIMD{0xb5f1'2590'5645'0b6c, 0x333a'2c49'c361'9e21};
    auto f = x.fold16(SIMD(0xbecc'9dd9'038f'c366, 0x5ba9'365b'e2e9'5bf5));
    ASSERT_EQ(f, SIMD(0x4f55'42df'ef35'1810, 0x0c03'5bd6'70fc'5abd));
}

TEST(CRC64, Fold8)
{
    if (!check_basic_support())
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto x = SIMD(0x60c0'b48f'4a92'2003, 0x203c'f7bc'ad34'103b);
    auto f = x.fold8(0x3e90'3688'ea71'f472);
    ASSERT_EQ(f, SIMD(0x07d7'2761'4d16'56db, 0x2bc0'ed8a'a341'7665));
}

TEST(CRC64, Barrett)
{
    if (!check_basic_support())
    {
        return GTEST_MESSAGE_("skipped", ::testing::TestPartResult::kSuccess);
    }
    auto x = SIMD(0x2606'e582'3406'9bae, 0x76cc'1105'0fef'6d68);
    auto b = x.barrett(0x435d'0f79'19a6'1445, 0x5817'6272'f8fa'b8d5);
    ASSERT_EQ(b, 0x5e4d'0253'942a'd95dULL);
}

struct CRC64 : ::testing::TestWithParam<crc64::Mode>
{
};

TEST_P(CRC64, Simple)
{
    FullFlagsGuard _guard;
    auto cases = Cases<std::vector<char>, uint64_t>{
        {{}, 0},
        {std::vector<char>(1, '@'), 0x7b1b'8ab9'8fa4'b8f8},
        {std::vector<char>{'1', '\x97'}, 0xfeb8'f7a1'ae3b'9bd4},
        {std::vector<char>{'M', '\"', '\xdf'}, 0xc016'0ce8'dd46'74d3},
        {std::vector<char>{'l', '\xcd', '\x13', '\xd7'}, 0x5c60'a6af'8299'6ea8},
        {std::vector<char>(32, 0), 0xc95a'f861'7cd5'330c},
        {std::vector<char>(32, -1), 0xe95d'ce9e'faa0'9acf},
        {{'\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x09', '\x0A',
          '\x0B', '\x0C', '\x0D', '\x0E', '\x0F', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15',
          '\x16', '\x17', '\x18', '\x19', '\x1A', '\x1B', '\x1C', '\x1D', '\x1E', '\x1F'},
         0x7fe5'71a5'8708'4d10},
        {std::vector<char>(1024, 0), 0xc378'6397'2069'270c}};
    for (auto [x, y] : cases)
    {
        auto simd = crc64::Digest(GetParam());
        simd.update(x.data(), x.size());
        ASSERT_EQ(simd.checksum(), y) << " auto mode, hex: " << std::hex << y;
        auto table = crc64::Digest(crc64::Mode::Table);
        table.update(x.data(), x.size());
        ASSERT_EQ(table.checksum(), y) << " table mode, hex: " << std::hex << y;
    }
}

TEST_P(CRC64, Random)
{
    FullFlagsGuard _guard;
    auto dev = std::random_device{};
    auto seed = dev();
    auto eng = std::mt19937_64{seed};
    auto dist = std::uniform_int_distribution<std::int8_t>{};
    for (auto i = 0; i < 1000; ++i)
    {
        std::vector<char> data;
        auto length = 10000 + dist(eng) % 1'0000'0000;
        data.resize(length);
        for (auto t = 0; t < length; ++t)
        {
            data[i] = dist(eng);
        }
        auto simd = crc64::Digest(GetParam());
        auto table = crc64::Digest(crc64::Mode::Table);
        simd.update(data.data(), data.size());
        table.update(data.data(), data.size());
        ASSERT_EQ(table.checksum(), simd.checksum())
            << "random engine seeded with: 0x" << std::hex << seed << std::endl;
    }
}

TEST_P(CRC64, Alignment)
{
    FullFlagsGuard _guard;
    auto dev = std::random_device{};
    auto seed = dev();
    auto eng = std::mt19937_64{seed};
    auto dist = std::uniform_int_distribution<std::int8_t>{};
    auto data = std::vector<char>(8192);
    for (auto & i : data)
    {
        i = dist(eng);
    }
    auto digest = crc64::Digest{GetParam()};
    digest.update(data.data(), data.size());
    auto initial = digest.checksum();
    auto * storage = reinterpret_cast<char *>(::operator new(8192 * 2, std::align_val_t{1024}));
    SCOPE_EXIT({ ::operator delete(storage, std::align_val_t{1024}); });
    for (auto align = 1; align <= 600; ++align)
    {
        std::memcpy(storage + align, data.data(), data.size());
        auto inner = crc64::Digest{GetParam()};
        inner.update(storage + align, data.size());
        ASSERT_EQ( // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
            inner.checksum(),
            initial)
            << "random engine seeded with: 0x" << std::hex << seed << std::endl;
    }
}

TEST_P(CRC64, Consection)
{
    FullFlagsGuard _guard;
    auto dev = std::random_device{};
    auto seed = dev();
    auto eng = std::mt19937_64{seed};
    auto dist = std::uniform_int_distribution<std::int8_t>{};
    auto data = std::vector<char>(65536);
    auto a = crc64::Digest{crc64::Mode::Table};
    auto b = crc64::Digest{GetParam()};
    for (auto & i : data)
    {
        i = dist(eng);
    }
    auto acc = 0;
    for (size_t i = 1; acc + i <= data.size(); acc += i, i <<= 1)
    {
        a.update(data.data() + acc, i);
        b.update(data.data() + acc, i);
        ASSERT_EQ(a.checksum(), b.checksum()) << "random engine seeded with: 0x" << std::hex << seed << std::endl;
    }
}

std::string parmToName(const ::testing::TestParamInfo<crc64::Mode> & info)
{
    switch (info.param)
    {
    case Mode::Table:
        return "table";
    case Mode::Auto:
        return "auto";
    case Mode::SIMD_128:
        return "simd128";
    case Mode::SIMD_256:
        return "simd256";
    case Mode::SIMD_512:
        return "simd512";
    }
    return "";
}

INSTANTIATE_TEST_CASE_P(
    Parm,
    CRC64,
    testing::Values(
        crc64::Mode::Table,
        crc64::Mode::Auto,
        crc64::Mode::SIMD_128,
        crc64::Mode::SIMD_256,
        crc64::Mode::SIMD_512),
    parmToName);

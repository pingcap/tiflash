#include <common/StringRef.h>
#include <gtest/gtest.h>

#include <random>
#include <utility>

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
struct StringRefTest : ::testing::TestWithParam<std::pair<bool, bool>>
{
};
TEST_P(StringRefTest, CompareTrivial)
{
    TempOption _option(GetParam().first, GetParam().second);
    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123", "123"}, {"abc", "abc"}, {"\v\a\t\n213@3213", "\v\a\t\n213@3213"}, {std::string(1024, '@'), std::string(1024, '@')}})
    {
        ASSERT_EQ(StringRef(a), StringRef(b));
    }

    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123-", "-123"}, {"ab", "abc"}, {"\a\t\n213#3213", "\v\a\t\n213@3213"}, {std::string(1024, '@'), std::string(1024, '!')}})
    {
        ASSERT_NE(StringRef(a), StringRef(b));
    }
}


TEST_P(StringRefTest, CompareLongEq)
{
    using namespace simd_option;
    TempOption _option(GetParam().first, GetParam().second);
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64, std::align_val_t{128}));
    auto aligned2 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64 + 23, std::align_val_t{128}));

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    ASSERT_EQ(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
    ::operator delete (aligned1, std::align_val_t{128});
    ::operator delete (aligned2, std::align_val_t{128});
}

TEST_P(StringRefTest, CompareLongNe)
{
    using namespace simd_option;
    TempOption _option(GetParam().first, GetParam().second);
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64, std::align_val_t{128}));
    auto aligned2 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64 + 23, std::align_val_t{128}));

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

    ::operator delete (aligned1, std::align_val_t{128});
    ::operator delete (aligned2, std::align_val_t{128});
}

using Parm = std::pair<bool, bool>;
#define MAKE_PAIR(x, y) (std::make_pair(x, y))

std::string parmToName(const ::testing::TestParamInfo<Parm> & info)
{
    std::stringstream ss;
    ss << "avx_" << info.param.first << "_avx512_" << info.param.second;
    return ss.str();
}

INSTANTIATE_TEST_CASE_P(Parm, StringRefTest,
    testing::Values(MAKE_PAIR(false, false), MAKE_PAIR(false, true), MAKE_PAIR(true, false), MAKE_PAIR(true, true)), parmToName);

#endif

#if defined(TIFLASH_ENABLE_ASIMD_SUPPORT)

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
struct StringRefTest : ::testing::TestWithParam<bool>
{
};
TEST_P(StringRefTest, CompareTrivial)
{
    TempOption _option(GetParam());
    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123", "123"}, {"abc", "abc"}, {"\v\a\t\n213@3213", "\v\a\t\n213@3213"}, {std::string(1024, '@'), std::string(1024, '@')}})
    {
        ASSERT_EQ(StringRef(a), StringRef(b));
    }

    for (auto & [a, b] : std::vector<std::pair<std::string, std::string>>{
             {"123-", "-123"}, {"ab", "abc"}, {"\a\t\n213#3213", "\v\a\t\n213@3213"}, {std::string(1024, '@'), std::string(1024, '!')}})
    {
        ASSERT_NE(StringRef(a), StringRef(b));
    }
}


TEST_P(StringRefTest, CompareLongEq)
{
    using namespace simd_option;
    TempOption _option(GetParam());
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64, std::align_val_t{128}));
    auto aligned2 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64 + 23, std::align_val_t{128}));

    aligned2 += 23;

    for (auto & i : data)
    {
        i = dist(eng);
    }

    strcpy(aligned1, data.data());
    strcpy(aligned2, data.data());

    ASSERT_EQ(StringRef(aligned1, data.size()), StringRef(aligned2, data.size())) << " seed: " << seed;
    ::operator delete (aligned1, std::align_val_t{128});
    ::operator delete (aligned2, std::align_val_t{128});
}

TEST_P(StringRefTest, CompareLongNe)
{
    using namespace simd_option;
    TempOption _option(GetParam());
    std::random_device device{};
    auto seed = device();
    std::default_random_engine eng{seed};
    std::uniform_int_distribution<char> dist(1, 'z');
    std::string data(1024 * 1024 * 64, ' ');

    auto aligned1 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64, std::align_val_t{128}));
    auto aligned2 = reinterpret_cast<char *>(::operator new (1024 * 1024 * 64 + 23, std::align_val_t{128}));

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
    ::operator delete (aligned1, std::align_val_t{128});
    ::operator delete (aligned2, std::align_val_t{128});
}

std::string parmToName(const ::testing::TestParamInfo<bool> & info)
{
    if (info.param)
        return "asimd";
    return "generic";
}

INSTANTIATE_TEST_CASE_P(bool, StringRefTest, testing::Values(false, true), parmToName);

#endif

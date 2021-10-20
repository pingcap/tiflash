#include <common/types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <random>
#if __SSE2__
#include <emmintrin.h>
#endif
#include <Functions/FunctionsString.h>

namespace DB
{
template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst);
};
} // namespace DB

namespace DB
{
namespace OldImpl
{
template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        const auto flip_case_mask = 'A' ^ 'a';

#if __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto * const src_end_sse = src_end - (src_end - src) % bytes_sse;

        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
        {
            /// load 16 sequential 8-bit characters
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

            /// keep `flip_case_mask` only where necessary, zero out elsewhere
            const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

            /// flip case by applying calculated mask
            const auto cased_chars = _mm_xor_si128(chars, xor_mask);

            /// store result back to destination
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
struct LowerUpperUTF8Impl
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';
    static void array(
        const UInt8 * src,
        const UInt8 * src_end,
        UInt8 * dst)
    {
#if __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto * src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

        /// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
        const auto v_zero = _mm_setzero_si128();
        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        while (src < src_end_sse)
        {
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// check for ASCII
            const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
            const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

            /// ASCII
            if (mask_is_not_ascii == 0)
            {
                const auto is_not_case
                    = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
                const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

                /// everything in correct case ASCII
                if (mask_is_not_case == 0)
                    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
                else
                {
                    /// ASCII in mixed case
                    /// keep `flip_case_mask` only where necessary, zero out elsewhere
                    const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                    /// flip case by applying calculated mask
                    const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                    /// store result back to destination
                    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
                }

                src += bytes_sse, dst += bytes_sse;
            }
            else
            {
                /// UTF-8
                const auto * const expected_end = src + bytes_sse;

                while (src < expected_end)
                    ::DB::LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(src, src_end, dst);

                /// adjust src_end_sse by pushing it forward or backward
                const auto diff = src - expected_end;
                if (diff != 0)
                {
                    if (src_end_sse + diff < src_end)
                        src_end_sse += diff;
                    else
                        src_end_sse -= bytes_sse - diff;
                }
            }
        }
#endif
        /// handle remaining symbols
        while (src < src_end)
            ::DB::LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(src, src_end, dst);
    }
};

} // namespace OldImpl

namespace tests
{
using LowerUtf8Impl = LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>;
using UpperUtf8Impl = LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>;
using LowerUtf8OldImpl = OldImpl::LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>;
using UpperUtf8OldImpl = OldImpl::LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>;
using LowerAsciiImpl = LowerUpperImpl<'A', 'Z'>;
using UpperAsciiImpl = LowerUpperImpl<'a', 'z'>;
using LowerAsciiOldImpl = OldImpl::LowerUpperImpl<'A', 'Z'>;
using UpperAsciiOldImpl = OldImpl::LowerUpperImpl<'a', 'z'>;
TEST(StringsLowerUpperUtf8, Simple)
{
    std::vector<std::string> test = {
        "你好",
        "hello",
        "ABCDEFasdsa21313!@$@#",
        "\xF0\x9F\x98\x80超级无敌abcABC",
        std::string(1024, 'a'),
        std::string(1024, '@'),
        std::string(1024, 'A')};

    //lower case
    {
        std::vector<std::string> res_new;
        std::vector<std::string> res_old;
        for (auto i : test)
        {
            res_new.push_back(i);
            LowerUtf8Impl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_new.back().data()));
        }
        for (auto i : test)
        {
            res_old.push_back(i);
            LowerUtf8OldImpl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_old.back().data()));
        }
        EXPECT_EQ(res_new, res_old);
    }

    //upper case
    {
        std::vector<std::string> res_new;
        std::vector<std::string> res_old;
        for (auto i : test)
        {
            res_new.push_back(i);
            UpperUtf8Impl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_new.back().data()));
        }
        for (auto i : test)
        {
            res_old.push_back(i);
            UpperUtf8OldImpl ::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_old.back().data()));
        }
        EXPECT_EQ(res_new, res_old);
    }
}


TEST(StringsLowerUpperAscii, Simple)
{
    std::vector<std::string> test = {
        "hello",
        "ABCDEFasdsa21313!@$@#",
        "abcABCdasfcioanfw 239miq mfiewa 0e- rc,q0xedsajckmcklcZXKMaskmdqwioj",
        std::string(1024, 'a'),
        std::string(1024, '@'),
        std::string(1024, 'A')};

    //lower case
    {
        std::vector<std::string> res_new;
        std::vector<std::string> res_old;
        for (auto i : test)
        {
            res_new.push_back(i);
            LowerAsciiImpl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_new.back().data()));
        }
        for (auto i : test)
        {
            res_old.push_back(i);
            LowerAsciiOldImpl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_old.back().data()));
        }
        EXPECT_EQ(res_new, res_old);
    }

    //upper case
    {
        std::vector<std::string> res_new;
        std::vector<std::string> res_old;
        for (auto i : test)
        {
            res_new.push_back(i);
            UpperAsciiImpl::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_new.back().data()));
        }
        for (auto i : test)
        {
            res_old.push_back(i);
            UpperAsciiOldImpl ::array(
                reinterpret_cast<const UInt8 *>(i.data()),
                reinterpret_cast<const UInt8 *>(i.data() + i.size()),
                reinterpret_cast<UInt8 *>(res_old.back().data()));
        }
        EXPECT_EQ(res_new, res_old);
    }
}


TEST(StringsLowerUpperAscii, Random)
{
    using namespace std::chrono;
    size_t limit = 1024 * 1024 * 64;
    std::random_device device;
    auto seed = device();
    std::cout << "seeded with: " << seed << std::endl;
    std::vector<UInt8> data(limit);
    std::vector<UInt8> res_new(limit);
    std::vector<UInt8> res_old(limit);
    std::default_random_engine eng(seed);
    std::uniform_int_distribution<UInt8> dist(
        'A',
        'Z');
    for (auto & i : data)
    {
        i = dist(eng);
    }
    {
        {
            auto begin = high_resolution_clock::now();
            LowerAsciiImpl ::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            LowerAsciiOldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }
}


TEST(StringsLowerUpperUtf8, Random)
{
    using namespace std::chrono;
    size_t limit = 1024 * 1024 * 64;
    std::random_device device;
    auto seed = device();
    std::cout << "seeded with: " << seed << std::endl;
    std::vector<UInt8> data(limit);
    std::vector<UInt8> res_new(limit, 0);
    std::vector<UInt8> res_old(limit, 0);
    std::default_random_engine eng(seed);
    std::uniform_int_distribution<UInt8> dist(
        std::numeric_limits<UInt8>::min(),
        std::numeric_limits<UInt8>::max());
    for (auto & i : data)
    {
        i = dist(eng);
    }
    {
        {
            auto begin = high_resolution_clock::now();
            LowerUtf8Impl::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "(lower) size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            LowerUtf8OldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "(lower) size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }

    {
        {
            auto begin = high_resolution_clock::now();
            UpperUtf8Impl::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "(upper) size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            UpperUtf8OldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "(upper) size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns" << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }
}

} // namespace tests

} // namespace DB
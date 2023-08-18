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

const char8_t * UNIT_TEST_RUSSIAN_ALPHABET
    = u8"ѨѩѬѭѦѧѪѫѠѡѰѱѮѯЅѕѴѵѲѳѢѣІіЯяЮюЭэЬьЫыЪъЩщШшЧчЦцХхФфУуТтСсРрПпОоНнМмЛлКкЙйИиЗзЖжЁёЕеДдГгВвБбАа";
const char8_t * UNIT_TEST_ENGLISH_ALPHABET
    = u8"A a B b C c D d E e F f G g H h I i J j K k L l M m N n O o P p Q q R r S s T t U u V v W w X x Y y Z z";
const char8_t * UNIT_TEST_GREEK_ALPHABET = u8"Α α, Β β, Γ γ, Δ δ, Ε ε, Ζ ζ, Η η, Θ θ, Ι ι, Κ κ, Λ λ, Μ μ, Ν ν, Ξ ξ, Ο "
                                           u8"ο, Π π, Ρ ρ, Σ σ/ς, Τ τ, Υ υ, Φ φ, Χ χ, Ψ ψ, and Ω ω.";
const char8_t * UNIT_TEST_CHINESE_PARAGRAPH
    = u8"PingCAP 成立于 2015 "
      u8"年，是一家企业级开源分布式数据库厂商，提供包括开源分布式数据库产品、解决方案与咨询、技术支持与培训认证服务，致"
      u8"力于为全球行业用户提供稳定高效、安全可靠、开放兼容的新型数据服务平台，"
      u8"解放企业生产力，加速企业数字化转型升级。在帮助企业释放增长空间的同时，也提供了一份具有高度可参考性的开源建设实"
      u8"践样本。"
      u8"由 PingCAP 创立的分布式关系型数据库 "
      u8"TiDB，为企业关键业务打造，具备「分布式强一致事务、在线弹性水平扩展、故障自恢复的高可用、跨数据中心多活」等企业"
      u8"级核心特性，帮助企业最大化发挥数据价值，充分释放企业增长空间。";
const char8_t * UNIT_TEST_JAPANESE_PARAGRAPH
    = u8"PingCAPは2015年に3人のインフラストラクチャエンジニアによってスタートしました。 "
      u8"3人はインターネット企業のデータベース管理者として、データベースの管理・スケーリング・運用・保守の業務に莫大な"
      u8"手間と時間に日々頭を抱えておりました。"
      u8"市場に良い解決策がないため、彼らはオープンソースで解決策を構築することに決めました。"
      u8"PingCAPは、一流のチームと世界中のコントリビューターの協力を得て、オープンソースの分散型NewSQLハイブリッドトラ"
      u8"ンザクションおよび分析処理（HTAP）データベースを構築しています。"
      u8"メインに開発したプロジェクトTiDBとは、MySQLと互換性のあるクラウドネイティブの分散SQLレイヤーであり、世界で最も"
      u8"人気のあるオープンソースデータベースプロジェクトの1つです。"
      u8"TiDBの関連プロジェクトTiKVは、クラウドネイティブの分散型Key-"
      u8"Valueストアです。現在CNCFの卒業プロジェクトになります。";
const char8_t * UNIT_TEST_ENGLISH_PARAGRAPH
    = u8"PingCAP started in 2015 when three seasoned infrastructure engineers were sick and tired of the way databases "
      u8"were managed, scaled, and maintained while working at leading Internet companies."
      u8"Seeing no good solution on the market, they decided to build one themselves — the open-source way."
      u8"With the help of a first-class team, and hundreds of contributors from around the globe, PingCAP is building "
      u8"an open-source distributed NewSQL Hybrid Transactional and Analytical Processing (HTAP)"
      u8"database. TiDB, our flagship project, is a cloud-native distributed SQL layer with MySQL compatibility, and "
      u8"one of the most popular open-source database projects in the world (don’t take our word for"
      u8"it, check it out). TiDB’s sister project, TiKV, is a cloud-native distributed Key-Value store. It is now a "
      u8"CNCF Graduated project.";

template <class Gen>
std::u8string_view getRandomString(Gen & gen)
{
    size_t seed = gen() % 6;
    switch (seed)
    {
    case 0:
        return UNIT_TEST_RUSSIAN_ALPHABET;
    case 1:
        return UNIT_TEST_ENGLISH_ALPHABET;
    case 2:
        return UNIT_TEST_GREEK_ALPHABET;
    case 3:
        return UNIT_TEST_CHINESE_PARAGRAPH;
    case 4:
        return UNIT_TEST_JAPANESE_PARAGRAPH;
    case 5:
        return UNIT_TEST_ENGLISH_PARAGRAPH;
    default:
        __builtin_unreachable();
    }
}

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
            const auto is_not_case = _mm_and_si128(
                _mm_cmpgt_epi8(chars, v_not_case_lower_bound),
                _mm_cmplt_epi8(chars, v_not_case_upper_bound));

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

template <
    char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
struct LowerUpperUTF8Impl
{
    static constexpr auto flip_case_mask = 'A' ^ 'a';
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
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
                const auto is_not_case = _mm_and_si128(
                    _mm_cmpgt_epi8(chars, v_not_case_lower_bound),
                    _mm_cmplt_epi8(chars, v_not_case_upper_bound));
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
                    ::DB::LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::
                        toCase(src, src_end, dst);

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
            ::DB::LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(
                src,
                src_end,
                dst);
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
    std::vector<std::string> test
        = {"你好",
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
    std::vector<std::string> test
        = {"hello",
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
    size_t limit = 1024 * 512;
    std::random_device device;
    auto seed = device();
    std::cout << "seeded with: " << seed << std::endl;
    std::vector<UInt8> data(limit + 1);
    std::vector<UInt8> res_new(limit + 1, 0);
    std::vector<UInt8> res_old(limit + 1, 0);
    std::default_random_engine eng(seed);
    std::uniform_int_distribution<UInt8> dist('A', 'Z');
    for (auto & i : data)
    {
        i = dist(eng);
    }
    data.back() = 0;
    {
        {
            auto begin = high_resolution_clock::now();
            LowerAsciiImpl ::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns"
                      << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            LowerAsciiOldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count() << " ns"
                      << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }
}


TEST(StringsLowerUpperUtf8, Random)
{
    using namespace std::chrono;
    size_t limit = 1024 * 512;
    std::random_device device;
    auto seed = device();
    std::cout << "seeded with: " << seed << std::endl;
    std::vector<UInt8> data(limit + 1);
    std::vector<UInt8> res_new(limit + 1, 0);
    std::vector<UInt8> res_old(limit + 1, 0);
    std::default_random_engine eng(seed);
    std::uniform_int_distribution<UInt8> dist('A', 'z');
    size_t size = 0;
    size_t target = data.size() - 1;
    while (size < target)
    {
        auto t = getRandomString(eng);
        if (t.size() > target - size)
        {
            data[size++] = dist(eng);
        }
        else
        {
            std::copy(t.begin(), t.end(), data.begin() + size);
            size += t.size();
        }
    }
    {
        {
            auto begin = high_resolution_clock::now();
            LowerUtf8Impl::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "(lower) size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count()
                      << " ns" << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            LowerUtf8OldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "(lower) size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count()
                      << " ns" << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }

    {
        {
            auto begin = high_resolution_clock::now();
            UpperUtf8Impl::array(data.data(), data.data() + limit, res_new.data());
            auto end = high_resolution_clock::now();
            std::cout << "(upper) size: " << limit << ", new impl: " << duration_cast<nanoseconds>(end - begin).count()
                      << " ns" << std::endl;
        }
        {
            auto begin = high_resolution_clock::now();
            UpperUtf8OldImpl::array(data.data(), data.data() + limit, res_old.data());
            auto end = high_resolution_clock::now();
            std::cout << "(upper) size: " << limit << ", old impl: " << duration_cast<nanoseconds>(end - begin).count()
                      << " ns" << std::endl;
        }
        EXPECT_EQ(res_new, res_old);
    }
}

} // namespace tests

} // namespace DB

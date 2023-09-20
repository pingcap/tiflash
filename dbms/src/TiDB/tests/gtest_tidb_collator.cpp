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

#include <Functions/CollationStringSearch.h>
#include <Functions/CollationStringSearchOptimized.h>
#include <TiDB/Collation/Collator.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{

using namespace TiDB;

struct CollatorCases
{
    enum
    {
        Bin = 0,
        BinPadding = 1,
        GeneralCI = 2,
        Utf8BinPadding = 3,
        UnicodeCI = 4,
        Utf8Mb40900AICI = 5,
        Utf8Mb40900Bin = 6,
    };
    template <typename T>
    using Answer = std::tuple<T, T, T, T, T, T, T>;
    using CompareCase = std::tuple<std::string, std::string, Answer<int>>;
    static const CompareCase cmp_cases[];

    using SortKeyCase = std::pair<std::string, Answer<std::string>>;
    static const SortKeyCase sk_cases[];

    using PatternCase = std::pair<std::string, std::vector<std::pair<std::string, Answer<bool>>>>;
    static const PatternCase pattern_cases[];
};
const typename CollatorCases::CompareCase CollatorCases::cmp_cases[] = {
    {"a", "b", {-1, -1, -1, -1, -1, -1, -1}},
    {"a", "A", {1, 1, 0, 1, 0, 0, 1}},
    {"À", "A", {1, 1, 0, 1, 0, 0, 1}},
    {"abc", "abc", {0, 0, 0, 0, 0, 0, 0}},
    {"abc", "ab", {1, 1, 1, 1, 1, 1, 1}},
    {"😜", "😃", {1, 1, 0, 1, 0, 1, 1}},
    {"a", "a ", {-1, 0, 0, 0, 0, -1, -1}},
    {"a ", "a  ", {-1, 0, 0, 0, 0, -1, -1}},
    {"a\t", "a", {1, 1, 1, 1, 1, 1, 1}},
    {"", "a", {-1, -1, -1, -1, -1, -1, -1}},
    {"a", "", {1, 1, 1, 1, 1, 1, 1}},
    {"ß", "ss", {1, 1, -1, 1, 0, 0, 1}},
    {"𐐭", "𐐨", {1, 1, 0, 1, 0, 1, 1}},
    // Issue https://github.com/pingcap/tics/issues/1660
    {"謺", "譂", {-1, -1, -1, -1, -1, -1, -1}},
};
#define PREVENT_TRUNC(s) \
    {                    \
        s, sizeof(s) - 1 \
    } // Prevent truncation by middle '\0' when constructing std::string using string literal, call std::string(const char *, size_t) instead.
const typename CollatorCases::SortKeyCase CollatorCases::sk_cases[] = {
    {"a",
     {PREVENT_TRUNC("\x61"),
      PREVENT_TRUNC("\x61"),
      PREVENT_TRUNC("\x00\x41"),
      PREVENT_TRUNC("\x61"),
      PREVENT_TRUNC("\x0e\x33"),
      PREVENT_TRUNC("\x1C\x47"),
      PREVENT_TRUNC("\x61")}},
    {"A",
     {PREVENT_TRUNC("\x41"),
      PREVENT_TRUNC("\x41"),
      PREVENT_TRUNC("\x00\x41"),
      PREVENT_TRUNC("\x41"),
      PREVENT_TRUNC("\x0e\x33"),
      PREVENT_TRUNC("\x1C\x47"),
      PREVENT_TRUNC("\x41")}},
    {"😃",
     {PREVENT_TRUNC("\xf0\x9f\x98\x83"),
      PREVENT_TRUNC("\xf0\x9f\x98\x83"),
      PREVENT_TRUNC("\xff\xfd"),
      PREVENT_TRUNC("\xf0\x9f\x98\x83"),
      PREVENT_TRUNC("\xff\xfd"),
      PREVENT_TRUNC("\x15\xFE"),
      PREVENT_TRUNC("\xf0\x9f\x98\x83")}},
    {"Foo © bar 𝌆 baz ☃ qux",
     {PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20"
                    "\x71\x75\x78"),
      PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20"
                    "\x71\x75\x78"),
      PREVENT_TRUNC("\x00\x46\x00\x4f\x00\x4f\x00\x20\x00\xa9\x00\x20\x00\x42\x00\x41\x00\x52\x00\x20\xff\xfd\x00\x20"
                    "\x00\x42\x00\x41"
                    "\x00\x5a\x00\x20\x26\x03\x00\x20\x00\x51\x00\x55\x00\x58"),
      PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20"
                    "\x71\x75\x78"),
      PREVENT_TRUNC("\x0E\xB9\x0F\x82\x0F\x82\x02\x09\x02\xC5\x02\x09\x0E\x4A\x0E\x33\x0F\xC0\x02\x09\xFF\xFD\x02\x09"
                    "\x0E\x4A\x0E\x33"
                    "\x10\x6A\x02\x09\x06\xFF\x02\x09\x0F\xB4\x10\x1F\x10\x5A"),
      PREVENT_TRUNC("\x1C\xE5\x1D\xDD\x1D\xDD\x02\x09\x05\x84\x02\x09\x1C\x60\x1C\x47\x1E\x33\x02\x09\x0E\xF0\x02\x09"
                    "\x1C\x60\x1C\x47"
                    "\x1F\x21\x02\x09\x09\x1B\x02\x09\x1E\x21\x1E\xB5\x1E\xFF"),
      PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20"
                    "\x71\x75\x78")}},
    {"a ",
     {PREVENT_TRUNC("\x61\x20"),
      PREVENT_TRUNC("\x61"),
      PREVENT_TRUNC("\x00\x41"),
      PREVENT_TRUNC("\x61"),
      PREVENT_TRUNC("\x0e\x33"),
      PREVENT_TRUNC("\x1C\x47\x02\x09"),
      PREVENT_TRUNC("\x61\x20")}},
    {"",
     {PREVENT_TRUNC(""),
      PREVENT_TRUNC(""),
      PREVENT_TRUNC(""),
      PREVENT_TRUNC(""),
      PREVENT_TRUNC(""),
      PREVENT_TRUNC(""),
      PREVENT_TRUNC("")}},
    {"ß",
     {PREVENT_TRUNC("\xc3\x9f"),
      PREVENT_TRUNC("\xc3\x9f"),
      PREVENT_TRUNC("\x00\x53"),
      PREVENT_TRUNC("\xc3\x9f"),
      PREVENT_TRUNC("\x0F\xEA\x0F\xEA"),
      PREVENT_TRUNC("\x1E\x71\x1E\x71"),
      PREVENT_TRUNC("\xc3\x9f")}},
};
const typename CollatorCases::PatternCase CollatorCases::pattern_cases[] = {
    {"A",
     {{"a", {false, false, true, false, true, true, false}},
      {"A", {true, true, true, true, true, true, true}},
      {"À", {false, false, true, false, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"_A",
     {{"aA", {true, true, true, true, true, true, true}},
      {"ÀA", {false, false, true, true, true, true, false}},
      {"ÀÀ", {false, false, true, false, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"%A",
     {{"a", {false, false, true, false, true, true, false}},
      {"ÀA", {true, true, true, true, true, true, true}},
      {"À", {false, false, true, false, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"À",
     {{"a", {false, false, true, false, true, true, false}},
      {"A", {false, false, true, false, true, true, false}},
      {"À", {true, true, true, true, true, true, true}},
      {"", {false, false, false, false, false, false, false}}}},
    {"_À",
     {{" À", {true, true, true, true, true, true, true}},
      {"ÀA", {false, false, true, false, true, true, false}},
      {"ÀÀ", {false, false, true, true, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"%À",
     {{"À", {true, true, true, true, true, true, true}},
      {"ÀÀÀ", {true, true, true, true, true, true, true}},
      {"ÀA", {false, false, true, false, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"À_",
     {{"À ", {true, true, true, true, true, true, true}},
      {"ÀAA", {false, false, false, false, false, false, false}},
      {"À", {false, false, false, false, false, false, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"À%",
     {{"À", {true, true, true, true, true, true, true}},
      {"ÀÀÀ", {true, true, true, true, true, true, true}},
      {"AÀ", {false, false, true, false, true, true, false}},
      {"", {false, false, false, false, false, false, false}}}},
    {"",
     {{"À", {false, false, false, false, false, false, false}},
      {"ÀÀÀ", {false, false, false, false, false, false, false}},
      {"AÀ", {false, false, false, false, false, false, false}},
      {"", {true, true, true, true, true, true, true}}}},
    {"%",
     {{"À", {true, true, true, true, true, true, true}},
      {"ÀÀÀ", {true, true, true, true, true, true, true}},
      {"AÀ", {true, true, true, true, true, true, true}},
      {"", {true, true, true, true, true, true, true}}}},
    {"a_%À",
     {{"ÀÀ", {false, false, false, false, false, false, false}},
      {"aÀÀ", {true, true, true, true, true, true, true}},
      {"ÀÀÀÀ", {false, false, true, false, true, true, false}},
      {"ÀÀÀa", {false, false, true, false, true, true, false}}}},
    {"À%_a",
     {{"ÀÀ", {false, false, false, false, false, false, false}},
      {"aÀÀ", {false, false, true, false, true, true, false}},
      {"ÀÀÀa", {true, true, true, true, true, true, true}},
      {"aÀÀÀ", {false, false, true, false, true, true, false}}}},
    {"___a",
     {{"中a", {true, true, false, false, false, false, true}},
      {"中文字a", {false, false, true, true, true, true, false}}}},
    {"𐐭", {{"𐐨", {false, false, true, false, false, false, false}}}},
    {
        "%pending%deposits%",
        {
            {"riously after the carefully pending foxes. deposits are careful",
             {true, true, true, true, true, true, true}},
            {"pendingdeposits", {true, true, true, true, true, true, true}},
            {"pendingdeposits", {true, true, true, true, true, true, true}},
        },
    },
    {
        "1234567\\", // `ESCAPE` at last
        {
            {"1234567\\", {true, true, true, true, true, true, true}},
            {"1234567", {false, false, false, false, false, false, false}},
            {"1234567\\1", {false, false, false, false, false, false, false}},
        },
    },
    {
        "1234567\\910", // `ESCAPE` at middle
        {
            {"1234567\\910", {false, false, false, false, false, false, false}},
            {"1234567910", {true, true, true, true, true, true, true}},
        },
    },
    {
        "%__", // test match from end
        {
            {"1", {false, false, false, false, false, false, false}}, // 1 bytes
            {"À", {true, true, false, false, false, false, true}}, // 2 bytes
            {"12", {true, true, true, true, true, true, true}}, // 2 bytes
            {"中", {true, true, false, false, false, false, true}}, // 3 bytes
            {"À1", {true, true, true, true, true, true, true}}, // 3 bytes
            {"ÀÀ", {true, true, true, true, true, true, true}}, // 4 bytes
            {"𒀈", {true, true, false, false, false, false, true}}, // 4 bytes 1 char
            {"À中", {true, true, true, true, true, true, true}}, // 5 bytes
            {"中中", {true, true, true, true, true, true, true}}, // 6 bytes
        },
    },
    {
        "%__%", // test
        {
            {"1", {false, false, false, false, false, false, false}}, // 1 bytes
            {"À", {true, true, false, false, false, false, true}}, // 2 bytes
            {"12", {true, true, true, true, true, true, true}}, // 2 bytes
            {"中", {true, true, false, false, false, false, true}}, // 3 bytes
            {"À1", {true, true, true, true, true, true, true}}, // 3 bytes
            {"ÀÀ", {true, true, true, true, true, true, true}}, // 4 bytes
            {"𒀈", {true, true, false, false, false, false, true}}, // 4 bytes 1 char
        },
    },
    {
        "%一_二", // test match from end
        {
            {"xx一a二", {true, true, true, true, true, true, true}},
            {"xx一À二", {false, false, true, true, true, true, false}},
        },
    },
    {
        "%一_三%四五六%七",
        {
            {"一二三四五七", {false, false, false, false, false, false, false}},
            {"0一二三四五六.七", {false, false, true, true, true, true, false}},
            {"一二四五六七", {false, false, false, false, false, false, false}},
            {"一2三.四五六...七", {true, true, true, true, true, true, true}},
        },
    },
    {
        "%一_三%",
        {
            {"000一二3", {false, false, false, false, false, false, false}},
            {"000一", {false, false, false, false, false, false, false}},
        },
    },
};

static constexpr char ESCAPE = '\\';

template <typename Collator>
void testCollator()
{
    const auto collator = ITiDBCollator::getCollator(Collator::collation);
    for (const auto & c : CollatorCases::cmp_cases)
    {
        const std::string & s1 = std::get<0>(c);
        const std::string & s2 = std::get<1>(c);
        int ans = std::get<Collator::collation_case>(std::get<2>(c));
        std::cout << "Compare case (" << s1 << ", " << s2 << ", " << ans << ")" << std::endl;
        ASSERT_EQ(signum((collator->compare(s1.data(), s1.length(), s2.data(), s2.length()))), ans);
    }
    for (const auto & c : CollatorCases::sk_cases)
    {
        const std::string & s = c.first;
        const std::string & ans = std::get<Collator::collation_case>(c.second);
        std::cout << "Sort key case (" << s << ", " << ans << ")" << std::endl;
        std::string buf;
        ASSERT_EQ(collator->sortKey(s.data(), s.length(), buf).toString(), ans);
    }
    {
        TiDB::BinStrPattern<true> matcher;
        matcher.compile("%%%", '%');
        ASSERT_TRUE(matcher.match("%%"));
        matcher.compile("%%", '.');
        ASSERT_TRUE(matcher.match(""));

        auto pattern = collator->pattern();
        pattern->compile("%%%", '%');
        ASSERT_TRUE(pattern->match("%%", 2));
    }
    {
        auto pattern = collator->pattern();
        for (const auto & c : CollatorCases::pattern_cases)
        {
            const std::string & p = c.first;
            const auto & inner_cases = c.second;

            ColumnString::Chars_t strs;
            ColumnString::Offsets offsets;
            PaddedPODArray<UInt8> res;
            { // init data
                ColumnString::Offset current_new_offset = 0;
                for (const auto & inner_c : inner_cases)
                {
                    const auto s = inner_c.first + char(0);
                    {
                        current_new_offset += s.size();
                        offsets.push_back(current_new_offset);
                    }
                    {
                        strs.resize(strs.size() + s.size());
                        std::memcpy(&strs[strs.size() - s.size()], s.data(), s.size());
                    }
                    res.emplace_back(0);
                }
            }
            if (!StringPatternMatch<false>(strs, offsets, p, ESCAPE, collator, res))
            {
                pattern->compile(p, ESCAPE);
                for (size_t idx = 0; idx < std::size(inner_cases); ++idx)
                {
                    const auto & inner_c = inner_cases[idx];
                    const std::string & s = inner_c.first;
                    res[idx] = pattern->match(s.data(), s.length());
                }
            }

            for (size_t idx = 0; idx < std::size(inner_cases); ++idx)
            {
                const auto & inner_c = inner_cases[idx];
                bool ans = std::get<Collator::collation_case>(inner_c.second);
                std::cout << "Pattern case (" << p << ", " << inner_c.first << ", " << ans << ")" << std::endl;
                ASSERT_EQ(res[idx], ans);
            }
        }
    }
}

struct BinCollator
{
    static constexpr int collation = ITiDBCollator::BINARY;
    static constexpr auto collation_case = CollatorCases::Bin;
};

struct BinPaddingCollator
{
    static constexpr int collation = ITiDBCollator::ASCII_BIN;
    static constexpr auto collation_case = CollatorCases::BinPadding;
};

struct Utf8BinPaddingCollator
{
    static constexpr int collation = ITiDBCollator::UTF8MB4_BIN;
    static constexpr auto collation_case = CollatorCases::Utf8BinPadding;
};

struct GeneralCICollator
{
    static constexpr int collation = ITiDBCollator::UTF8MB4_GENERAL_CI;
    static constexpr auto collation_case = CollatorCases::GeneralCI;
};

struct UnicodeCICollator
{
    static constexpr int collation = ITiDBCollator::UTF8MB4_UNICODE_CI;
    static constexpr auto collation_case = CollatorCases::UnicodeCI;
};

struct Utf8Mb40900AICICollator
{
    static constexpr int collation = ITiDBCollator::UTF8MB4_0900_AI_CI;
    static constexpr auto collation_case = CollatorCases::Utf8Mb40900AICI;
};

struct Utf8Mb40900BinCollator
{
    static constexpr int collation = ITiDBCollator::UTF8MB4_0900_BIN;
    static constexpr auto collation_case = CollatorCases::Utf8Mb40900Bin;
};

TEST(CollatorSuite, BinCollator)
{
    testCollator<BinCollator>();
}

TEST(CollatorSuite, BinPaddingCollator)
{
    testCollator<BinPaddingCollator>();
}

TEST(CollatorSuite, Utf8BinPaddingCollator)
{
    testCollator<Utf8BinPaddingCollator>();
}

TEST(CollatorSuite, GeneralCICollator)
{
    testCollator<GeneralCICollator>();
}

TEST(CollatorSuite, UnicodeCICollator)
{
    testCollator<UnicodeCICollator>();
}

TEST(CollatorSuite, Utf8Mb40900AICICollator)
{
    testCollator<Utf8Mb40900AICICollator>();
}

TEST(CollatorSuite, Utf8Mb40900BinCollator)
{
    testCollator<Utf8Mb40900AICICollator>();
}

} // namespace DB::tests

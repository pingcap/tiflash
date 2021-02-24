#include <Storages/Transaction/Collator.h>
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
    };
    template <typename T>
    using Answer = std::tuple<T, T, T, T>;
    using CompareCase = std::tuple<std::string, std::string, Answer<int>>;
    static const CompareCase cmp_cases[];

    using SortKeyCase = std::pair<std::string, Answer<std::string>>;
    static const SortKeyCase sk_cases[];

    using PatternCase = std::pair<std::string, std::vector<std::pair<std::string, Answer<bool>>>>;
    static const PatternCase pattern_cases[];
};
const typename CollatorCases::CompareCase CollatorCases::cmp_cases[] = {
    {"a", "b", {-1, -1, -1, -1}},
    {"a", "A", {1, 1, 0, 1}},
    {"À", "A", {1, 1, 0, 1}},
    {"abc", "abc", {0, 0, 0, 0}},
    {"abc", "ab", {1, 1, 1, 1}},
    {"😜", "😃", {1, 1, 0, 1}},
    {"a", "a ", {-1, 0, 0, 0}},
    {"a ", "a  ", {-1, 0, 0, 0}},
    {"a\t", "a", {1, 1, 1, 1}},
    {"", "a", {-1, -1, -1, -1}},
    {"a", "", {1, 1, 1, 1}},
};
#define PREVENT_TRUNC(s) \
    {                    \
        s, sizeof(s) - 1 \
    } // Prevent truncation by middle '\0' when constructing std::string using string literal, call std::string(const char *, size_t) instead.
const typename CollatorCases::SortKeyCase CollatorCases::sk_cases[] = {
    {"a", {PREVENT_TRUNC("\x61"), PREVENT_TRUNC("\x61"), PREVENT_TRUNC("\x00\x41"), PREVENT_TRUNC("\x61")}},
    {"A", {PREVENT_TRUNC("\x41"), PREVENT_TRUNC("\x41"), PREVENT_TRUNC("\x00\x41"), PREVENT_TRUNC("\x41")}},
    {"😃", {PREVENT_TRUNC("\xf0\x9f\x98\x83"), PREVENT_TRUNC("\xf0\x9f\x98\x83"), PREVENT_TRUNC("\xff\xfd"), PREVENT_TRUNC("\xf0\x9f\x98\x83")}},
    {"Foo © bar 𝌆 baz ☃ qux",
        {PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20\x71\x75\x78"),
            PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20\x71\x75\x78"),
            PREVENT_TRUNC("\x00\x46\x00\x4f\x00\x4f\x00\x20\x00\xa9\x00\x20\x00\x42\x00\x41\x00\x52\x00\x20\xff\xfd\x00\x20\x00\x42\x00\x41"
                          "\x00\x5a\x00\x20\x26\x03\x00\x20\x00\x51\x00\x55\x00\x58"),
            PREVENT_TRUNC("\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20\x71\x75\x78")
        }},
    {"a ", {PREVENT_TRUNC("\x61\x20"), PREVENT_TRUNC("\x61"), PREVENT_TRUNC("\x00\x41"), PREVENT_TRUNC("\x61")}},
    {"", {PREVENT_TRUNC(""), PREVENT_TRUNC(""), PREVENT_TRUNC(""), PREVENT_TRUNC("")}},
};
const typename CollatorCases::PatternCase CollatorCases::pattern_cases[] = {
    {"A", {{"a", {false, false, true, false}}, {"A", {true, true, true, true}}, {"À", {false, false, true, false}}, {"", {false, false, false, false}}}},
    {"_A", {{"aA", {true, true, true, true}}, {"ÀA", {false, false, true, true}}, {"ÀÀ", {false, false, true, false}}, {"", {false, false, false, false}}}},
    {"%A", {{"a", {false, false, true, false}}, {"ÀA", {true, true, true, true}}, {"À", {false, false, true, false}}, {"", {false, false, false, false}}}},
    {"À", {{"a", {false, false, true, false}}, {"A", {false, false, true, false}}, {"À", {true, true, true, true}}, {"", {false, false, false, false}}}},
    {"_À", {{" À", {true, true, true, true}}, {"ÀA", {false, false, true, false}}, {"ÀÀ", {false, false, true, true}}, {"", {false, false, false, false}}}},
    {"%À", {{"À", {true, true, true, true}}, {"ÀÀÀ", {true, true, true, true}}, {"ÀA", {false, false, true, false}}, {"", {false, false, false, false}}}},
    {"À_", {{"À ", {true, true, true, true}}, {"ÀAA", {false, false, false, false}}, {"À", {false, false, false, false}}, {"", {false, false, false, false}}}},
    {"À%", {{"À", {true, true, true, true}}, {"ÀÀÀ", {true, true, true, true}}, {"AÀ", {false, false, true, false}}, {"", {false, false, false, false}}}},
    {"", {{"À", {false, false, false, false}}, {"ÀÀÀ", {false, false, false, false}}, {"AÀ", {false, false, false, false}}, {"", {true, true, true, true}}}},
    {"%", {{"À", {true, true, true, true}}, {"ÀÀÀ", {true, true, true, true}}, {"AÀ", {true, true, true, true}}, {"", {true, true, true, true}}}},
    {"a_%À", {{"ÀÀ", {false, false, false, false}}, {"aÀÀ", {true, true, true, true}}, {"ÀÀÀÀ", {false, false, true, false}}, {"ÀÀÀa", {false, false, true, false}}}},
    {"À%_a", {{"ÀÀ", {false, false, false, false}}, {"aÀÀ", {false, false, true, false}}, {"ÀÀÀa", {true, true, true, true}}, {"aÀÀÀ", {false, false, true, false}}}},
    {"___a", {{"中a", {true, true, false, false}}, {"中文字a", {false, false, true, true}}}},
};

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
        ASSERT_EQ(collator->compare(s1.data(), s1.length(), s2.data(), s2.length()), ans);
    }
    for (const auto & c : CollatorCases::sk_cases)
    {
        const std::string & s = c.first;
        const std::string & ans = std::get<Collator::collation_case>(c.second);
        std::cout << "Sort key case (" << s << ", " << ans << ")" << std::endl;
        std::string buf;
        ASSERT_EQ(collator->sortKey(s.data(), s.length(), buf).toString(), ans);
    }
    auto pattern = collator->pattern();
    for (const auto & c : CollatorCases::pattern_cases)
    {
        const std::string & p = c.first;
        pattern->compile(p, '\\');
        const auto & inner_cases = c.second;
        for (const auto & inner_c : inner_cases)
        {
            const std::string & s = inner_c.first;
            bool ans = std::get<Collator::collation_case>(inner_c.second);
            std::cout << "Pattern case (" << p << ", " << s << ", " << ans << ")" << std::endl;
            ASSERT_EQ(pattern->match(s.data(), s.length()), ans);
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

TEST(CollatorSuite, BinCollator) { testCollator<BinCollator>(); }

TEST(CollatorSuite, BinPaddingCollator) { testCollator<BinPaddingCollator>(); }

TEST(CollatorSuite, Utf8BinPaddingCollator) { testCollator<Utf8BinPaddingCollator>(); }

TEST(CollatorSuite, GeneralCICollator) { testCollator<GeneralCICollator>(); }

} // namespace DB::tests

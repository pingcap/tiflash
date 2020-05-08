#include <Storages/Transaction/Collator.h>
#include <gtest/gtest.h>

namespace DB::tests
{

using namespace TiDB;

template <typename Collator>
struct CollatorCases
{
    using CompareCase = std::tuple<std::string, std::string, int>;
    static const CompareCase cmp_cases[];

    using SortKeyCase = std::pair<std::string, const std::string &>;
    static const SortKeyCase sk_cases[];
};
template <typename Collator>
const typename CollatorCases<Collator>::CompareCase CollatorCases<Collator>::cmp_cases[] = {
    {"a", "b", Collator::cmp_ans[0]},
    {"a", "A", Collator::cmp_ans[1]},
    {"À", "A", Collator::cmp_ans[2]},
    {"abc", "abc", Collator::cmp_ans[3]},
    {"abc", "ab", Collator::cmp_ans[4]},
    {"😜", "😃", Collator::cmp_ans[5]},
    {"a", "a ", Collator::cmp_ans[6]},
    {"a ", "a  ", Collator::cmp_ans[7]},
    {"a\t", "a", Collator::cmp_ans[8]},
};
template <typename Collator>
const typename CollatorCases<Collator>::SortKeyCase CollatorCases<Collator>::sk_cases[] = {
    {"a", Collator::sk_ans[0]},
    {"A", Collator::sk_ans[1]},
    {"😃", Collator::sk_ans[2]},
    {"Foo © bar 𝌆 baz ☃ qux", Collator::sk_ans[3]},
    {"\x88\xe6", Collator::sk_ans[4]},
    {"a ", Collator::sk_ans[5]},
};

template <typename Collator>
void testCollator()
{
    const auto collator = ICollator::getCollator(Collator::collation);
    for (const auto & c : CollatorCases<Collator>::cmp_cases)
    {
        const std::string & s1 = std::get<0>(c);
        const std::string & s2 = std::get<1>(c);
        int ans = std::get<2>(c);
        std::cout << "Compare case (" << s1 << ", " << s2 << ", " << ans << ")" << std::endl;
        ASSERT_EQ(collator->compare(s1.data(), s1.length(), s2.data(), s2.length()), ans);
    }
    for (const auto & c : CollatorCases<Collator>::sk_cases)
    {
        const std::string & s = c.first;
        const std::string & ans = c.second;
        std::cout << "Sort key case (" << s << ", " << ans << ")" << std::endl;
        ASSERT_EQ(collator->sortKey(s.data(), s.length()), ans);
    }
}

struct BinCollator
{
    static constexpr int collation = ICollator::BINARY;
    static const int cmp_ans[];
    static const std::string sk_ans[];
};
const int BinCollator::cmp_ans[] = {
    -1,
    1,
    1,
    0,
    1,
    1,
    -1,
    -1,
    1,
};
const std::string BinCollator::sk_ans[] = {
    "\x61",
    "\x41",
    "\xf0\x9f\x98\x83",
    "\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20\x71\x75\x78",
    "\x88\xe6",
    "\x61\x20",
};

struct BinPaddingCollator
{
    static constexpr int collation = ICollator::ASCII_BIN;
    static const int cmp_ans[];
    static const std::string sk_ans[];
};
const int BinPaddingCollator::cmp_ans[] = {
    -1,
    1,
    1,
    0,
    1,
    1,
    0,
    0,
    1,
};
const std::string BinPaddingCollator::sk_ans[] = {
    "\x61",
    "\x41",
    "\xf0\x9f\x98\x83",
    "\x46\x6f\x6f\x20\xc2\xa9\x20\x62\x61\x72\x20\xf0\x9d\x8c\x86\x20\x62\x61\x7a\x20\xe2\x98\x83\x20\x71\x75\x78",
    "\x88\xe6",
    "\x61",
};

struct GeneralCICollator
{
    static constexpr int collation = ICollator::UTF8MB4_GENERAL_CI;
    static const int cmp_ans[];
    static const std::string sk_ans[];
};
const int GeneralCICollator::cmp_ans[] = {
    -1,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    1,
};
#define M(s)             \
    {                    \
        s, sizeof(s) - 1 \
    } // Prevent truncation by middle '\0' when constructing std::string using string literal, call std::string(const char *, size_t) instead.
const std::string GeneralCICollator::sk_ans[] = {
    M("\x00\x41"),
    M("\x00\x41"),
    M("\xff\xfd"),
    M("\x00\x46\x00\x4f\x00\x4f\x00\x20\x00\xa9\x00\x20\x00\x42\x00\x41\x00\x52\x00\x20\xff\xfd\x00\x20\x00\x42\x00\x41\x00\x5a\x00\x20\x26"
      "\x03\x00\x20\x00\x51\x00\x55\x00\x58"),
    M("\xff\xfd\xff\xfd"),
    M("\x00\x41"),
};

TEST(CollatorSuite, BinCollator) { testCollator<BinCollator>(); }

TEST(CollatorSuite, BinPaddingCollator) { testCollator<BinPaddingCollator>(); }

TEST(CollatorSuite, GeneralCICollator) { testCollator<GeneralCICollator>(); }

} // namespace DB::tests

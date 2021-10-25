#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class StringMatch : public FunctionTest
{
};

TEST_F(StringMatch, Like3ArgsVectorWithVector)
try
{
    /**
     * With LIKE you can use the following two wildcard characters in the pattern:
     * * % matches any number of characters, even zero characters.
     * * _ matches exactly one character.
     */
    struct Case
    {
        int match;
        std::string a;
        std::string b;
    };
    std::vector<Case> cases = {
        {1, "", ""},
        {1, "a", "a"},
        {1, "", ""},
        {1, "a", "%"},
        {1, "a", "a%"},
        {1, "a", "%a"},
        {1, "ab", "a%"},
        {1, "ab", "ab"},
        // pattern can only be used as the second argument
        {0, "a%", "ab"},
        {1, "aaaa", "a%"},
        {0, "aaaa", "aaab%"},
        {1, "aabaababaabbab", "aab%a%aab%b"},
        {1, "a", "_"},
        {1, "abab", "_b__"},
        {0, "abab", "_b_"},
    };

    InferredDataVector<Nullable<String>> haystack_raw = {};
    InferredDataVector<Nullable<String>> needle_raw = {};
    InferredDataVector<Nullable<UInt8>> result_raw = {};

    for (auto && cas : cases)
    {
        haystack_raw.push_back(cas.a);
        needle_raw.push_back(cas.b);
        result_raw.push_back(cas.match);
    }

    auto haystack = createColumn<Nullable<String>>(haystack_raw, "haystack");
    auto needle = createColumn<Nullable<String>>(needle_raw, "needle");
    auto escape = createConstColumn<Nullable<Int32>>(1, static_cast<Int32>('\\'));
    auto expected = createColumn<Nullable<UInt8>>(result_raw, "result");

    auto result = executeFunction("like3Args", {haystack, needle, escape});

    ASSERT_COLUMN_EQ(expected, result);
}
CATCH

TEST_F(StringMatch, Like3ArgsConstantWithVector)
try
{
    /**
     * With LIKE you can use the following two wildcard characters in the pattern:
     * * % matches any number of characters, even zero characters.
     * * _ matches exactly one character.
     */

    struct Case
    {
        std::string src;
        std::vector<std::pair<std::string, int>> pat;
    };
    std::vector<Case> cases = {
        {"a", {{"b", 0}, {"a", 1}, {"_", 1}, {"%", 1}}},
        {"aab", {{"aab", 1}, {"ab_", 0}, {"a_a", 0}, {"a__", 1}}},
    };

    for (auto && cas : cases)
    {
        InferredDataVector<Nullable<String>> needle_raw = {};
        InferredDataVector<Nullable<UInt8>> result_raw = {};

        for (auto && pat : cas.pat)
        {
            needle_raw.push_back(pat.first);
            result_raw.push_back(pat.second);
        }

        auto haystack = createConstColumn<Nullable<String>>(1, cas.src);
        auto needle = createColumn<Nullable<String>>(needle_raw);
        auto escape = createConstColumn<Nullable<Int32>>(1, static_cast<Int32>('\\'));
        auto expected = createColumn<Nullable<UInt8>>(result_raw);

        auto result = executeFunction("like3Args", {haystack, needle, escape});
        ASSERT_COLUMN_EQ(expected, result);
    }
}
CATCH
} // namespace tests

} // namespace DB

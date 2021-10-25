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

    InferredDataVector<String> haystack_raw = {};
    InferredDataVector<String> needle_raw = {};
    InferredDataVector<UInt8> result_raw = {};

    for (auto && cas : cases)
    {
        haystack_raw.push_back(cas.a);
        needle_raw.push_back(cas.b);
        result_raw.push_back(cas.match);
    }

    auto haystack = createColumn<String>(haystack_raw, "haystack");
    auto needle = createColumn<String>(needle_raw, "needle");
    auto escape = createConstColumn<Int32>(1, static_cast<Int32>('\\'));
    auto expected = createColumn<UInt8>(result_raw, "result");

    auto result = executeFunction("like3Args", {haystack, needle, escape});

    ASSERT_COLUMN_EQ(expected, result);

    struct NullableCase
    {
        std::optional<int> match;
        std::optional<std::string> a;
        std::optional<std::string> b;
    };
    std::vector<NullableCase> nullable_cases = {
            {std::nullopt, std::nullopt, ""},
            {std::nullopt, "a", std::nullopt},
            {std::nullopt, std::nullopt, std::nullopt},
            {1, "a", "%"},
    };

    InferredDataVector<Nullable<String>> nullable_haystack_raw = {};
    InferredDataVector<Nullable<String>> nullable_needle_raw = {};
    InferredDataVector<Nullable<UInt8>> nullable_result_raw = {};

    for (auto && cas : nullable_cases)
    {
        nullable_haystack_raw.push_back(cas.a);
        nullable_needle_raw.push_back(cas.b);
        nullable_result_raw.push_back(cas.match);
    }

    auto nullable_haystack = createColumn<Nullable<String>>(nullable_haystack_raw, "haystack");
    auto nullable_needle = createColumn<Nullable<String>>(nullable_needle_raw, "needle");
    auto nullable_escape = createConstColumn<Nullable<Int32>>(1, static_cast<Int32>('\\'));
    auto nullable_expected = createColumn<Nullable<UInt8>>(nullable_result_raw, "result");

    auto nullable_result = executeFunction("like3Args", {nullable_haystack, nullable_needle, nullable_escape});

    ASSERT_COLUMN_EQ(nullable_expected, nullable_result);
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

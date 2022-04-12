// Copyright 2022 PingCAP, Ltd.
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
protected:
    const String long_str = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzab"
                            "cdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef"
                            "ghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl"
                            "mnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqr"
                            "stuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvw"
                            "xyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

    const String long_pattern = "abcdefghijklmnopqrstuvwxyz_bcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz%abcdefghijklmnopqrstuvwxyz";

    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<UInt64>> & v)
    {
        return createColumn<Nullable<UInt8>>(v);
    }

    static ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        std::vector<String> strings;
        strings.reserve(v.size());
        for (std::optional<String> s : v)
        {
            strings.push_back(s.value());
        }
        return createColumn<String>(strings);
    }

    static ColumnWithTypeAndName toVec(const std::vector<std::optional<UInt64>> & v)
    {
        std::vector<UInt64> ints;
        ints.reserve(v.size());
        for (std::optional<UInt64> i : v)
        {
            ints.push_back(i.value());
        }
        return createColumn<UInt8>(ints);
    }

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }

    static ColumnWithTypeAndName toConst(const UInt8 i)
    {
        return createConstColumn<UInt8>(1, i);
    }
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

    for (auto & cas : cases)
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

    for (auto & cas : nullable_cases)
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

    for (auto & cas : cases)
    {
        InferredDataVector<Nullable<String>> needle_raw = {};
        InferredDataVector<Nullable<UInt8>> result_raw = {};

        for (auto & pat : cas.pat)
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

TEST_F(StringMatch, LikeVectorWithVector)
{
    std::vector<std::optional<String>> haystack = {"我爱tiflash", "我爱tiflash", "", "a", "", "a", "a", "a", "ab", "ab", "a%", "aaaa", "aaaa", "aabaababaabbab", "a", "abab", "abab", "abcdefghijklmn", "a", long_str};
    std::vector<std::optional<String>> needle = {"我_tif%", "%爱ti%", "", "a", "", "%", "a%", "%a", "a%", "ab", "ab", "a%", "aaab%", "aab%a%aab%b", "_", "_b__", "_b_", "a%", "abcdefghijklmn%", long_pattern};
    std::vector<std::optional<UInt64>> expect = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect),
        executeFunction(
            "like",
            toNullableVec(haystack),
            toNullableVec(needle)));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            "like",
            toVec(haystack),
            toVec(needle)));

    std::vector<std::optional<String>> haystack_null = {{}, "a"};
    std::vector<std::optional<String>> needle_null = {"我_tif%", {}};
    std::vector<std::optional<UInt64>> expect_null = {{}, {}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            "like",
            toNullableVec(haystack_null),
            toNullableVec(needle_null)));
}

TEST_F(StringMatch, LikeConstWithVector)
{
    std::vector<std::optional<String>> needle = {"", "a", "", "%", "a%", "%a", "a%", "ab", "ab", "a%", "aaab%", "aab%a%aab%b", "_", "_b__", "_b_", long_pattern};
    std::vector<std::optional<UInt64>> expect = {0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect1 = {0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect),
        executeFunction(
            "like",
            toConst("abcaba"),
            toNullableVec(needle)));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            "like",
            toConst("abcaba"),
            toVec(needle)));

    ASSERT_COLUMN_EQ(
        toVec(expect1),
        executeFunction(
            "like",
            toConst(long_str),
            toVec(needle)));

    std::vector<std::optional<String>> needle_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            "like",
            toConst("abc"),
            toNullableVec(needle_null)));
}

TEST_F(StringMatch, LikeVectorWithConst)
{
    std::vector<std::optional<String>> haystack = {"我爱tiflash", "", "a", "", "a", "a", "a", "ab", "ab", "a%", "aaaa", "aaaa", "aabaababaabbab", "a", "abab", "abab", long_str};
    std::vector<std::optional<UInt64>> expect = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect1 = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect3 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect),
        executeFunction(
            "like",
            toNullableVec(haystack),
            toConst("%aa%")));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            "like",
            toVec(haystack),
            toConst("%aa%")));

    ASSERT_COLUMN_EQ(
        toVec(expect1),
        executeFunction(
            "like",
            toVec(haystack),
            toConst("%爱tif%")));

    ASSERT_COLUMN_EQ(
        toVec(expect2),
        executeFunction(
            "like",
            toVec(haystack),
            toConst("%不爱tif%")));

    ASSERT_COLUMN_EQ(
        toVec(expect3),
        executeFunction(
            "like",
            toVec(haystack),
            toConst(long_pattern)));

    std::vector<std::optional<String>> haystack_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            "like",
            toNullableVec(haystack_null),
            toConst("abc")));
}

TEST_F(StringMatch, LikeConstWithConst)
{
    ASSERT_COLUMN_EQ(
        toConst(1),
        executeFunction(
            "like",
            toConst("resaasfe"),
            toConst("%aa%")));

    ASSERT_COLUMN_EQ(
        toConst(0),
        executeFunction(
            "like",
            toConst("abcde"),
            toConst("%aa%")));

    ASSERT_COLUMN_EQ(
        toConst(1),
        executeFunction(
            "like",
            toConst("我爱tiflash"),
            toConst("%爱tif%")));

    ASSERT_COLUMN_EQ(
        toConst(0),
        executeFunction(
            "like",
            toConst("我爱tiflash"),
            toConst("%不爱tif%")));
}

} // namespace tests
} // namespace DB

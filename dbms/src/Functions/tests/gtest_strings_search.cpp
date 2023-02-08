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
#include <Functions/FunctionsStringSearch.cpp>
#include "Storages/Transaction/Collator.h"
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class StringMatch : public FunctionTest
{
protected:
    const String func_like_name = "like3Args";
    const String func_ilike_name = "ilike3Args";
    const String long_str = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzab"
                            "cdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef"
                            "ghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl"
                            "mnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqr"
                            "stuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvw"
                            "xyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

    const String long_pattern = "abcdefghijklmnopqrstuvwxyz_bcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz%abcdefghijklmnopqrstuvwxyz";

    std::vector<TiDB::TiDBCollatorPtr> collators {
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_GENERAL_CI),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_UNICODE_CI),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN),
        TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_BIN)
    };

    ColumnWithTypeAndName escape = createConstColumn<Int32>(1, static_cast<Int32>('\\'));

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
    auto expected = createColumn<UInt8>(result_raw, "result");

    auto result = executeFunction(func_like_name, {haystack, needle, escape});

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
    auto nullable_expected = createColumn<Nullable<UInt8>>(nullable_result_raw, "result");

    auto nullable_result = executeFunction(func_like_name, {nullable_haystack, nullable_needle, escape});

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
        auto expected = createColumn<Nullable<UInt8>>(result_raw);

        auto result = executeFunction(func_like_name, {haystack, needle, escape});
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
            func_like_name,
            toNullableVec(haystack),
            toNullableVec(needle),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            func_like_name,
            toVec(haystack),
            toVec(needle),
            escape));

    std::vector<std::optional<String>> haystack_null = {{}, "a"};
    std::vector<std::optional<String>> needle_null = {"我_tif%", {}};
    std::vector<std::optional<UInt64>> expect_null = {{}, {}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            func_like_name,
            toNullableVec(haystack_null),
            toNullableVec(needle_null),
            escape));
}

TEST_F(StringMatch, LikeConstWithVector)
{
    std::vector<std::optional<String>> needle = {"", "a", "", "%", "a%", "%a", "a%", "ab", "ab", "a%", "aaab%", "aab%a%aab%b", "_", "_b__", "_b_", long_pattern};
    std::vector<std::optional<UInt64>> expect = {0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect1 = {0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect),
        executeFunction(
            func_like_name,
            toConst("abcaba"),
            toNullableVec(needle),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            func_like_name,
            toConst("abcaba"),
            toVec(needle),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect1),
        executeFunction(
            func_like_name,
            toConst(long_str),
            toVec(needle),
            escape));

    std::vector<std::optional<String>> needle_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            func_like_name,
            toConst("abc"),
            toNullableVec(needle_null),
            escape));
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
            func_like_name,
            toNullableVec(haystack),
            toConst("%aa%"),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            func_like_name,
            toVec(haystack),
            toConst("%aa%"),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect1),
        executeFunction(
            func_like_name,
            toVec(haystack),
            toConst("%爱tif%"),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect2),
        executeFunction(
            func_like_name,
            toVec(haystack),
            toConst("%不爱tif%"),
            escape));

    ASSERT_COLUMN_EQ(
        toVec(expect3),
        executeFunction(
            func_like_name,
            toVec(haystack),
            toConst(long_pattern),
            escape));

    std::vector<std::optional<String>> haystack_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect_null),
        executeFunction(
            func_like_name,
            toNullableVec(haystack_null),
            toConst("abc"),
            escape));
}

TEST_F(StringMatch, LikeConstWithConst)
{
    ASSERT_COLUMN_EQ(
        toConst(1),
        executeFunction(
            func_like_name,
            toConst("resaasfe"),
            toConst("%aa%"),
            escape));

    ASSERT_COLUMN_EQ(
        toConst(0),
        executeFunction(
            func_like_name,
            toConst("abcde"),
            toConst("%aa%"),
            escape));

    ASSERT_COLUMN_EQ(
        toConst(1),
        executeFunction(
            func_like_name,
            toConst("我爱tiflash"),
            toConst("%爱tif%"),
            escape));

    ASSERT_COLUMN_EQ(
        toConst(0),
        executeFunction(
            func_like_name,
            toConst("我爱tiflash"),
            toConst("%不爱tif%"),
            escape));
}

TEST_F(StringMatch, Ilike3ArgsVectorWithVector)
try
{
    struct Case
    {
        int match;
        std::string a;
        std::string b;
    };
    std::vector<Case> cases = {
        {1, "", ""},
        {1, "a", "A"},
        {1, "", ""},
        {1, "a", "%"},
        {1, "A", "a%"},
        {1, "a", "%a"},
        {1, "ab", "a%"},
        {1, "ab", "ab"},
        // pattern can only be used as the second argument
        {0, "a%", "ab"},
        {1, "aaaa", "a%"},
        {0, "aaaa", "aaab%"},
        {1, "aabAAbabaabbaB", "aab%a%aAB%b"},
        {1, "a", "_"},
        {1, "Abab", "_b__"},
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
    auto expected = createColumn<UInt8>(result_raw, "result");

    for (const auto * collator : collators)
    {
        auto result = executeFunction(func_ilike_name, {haystack, needle, escape}, collator);
        ASSERT_COLUMN_EQ(expected, result);
    }

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
    auto nullable_expected = createColumn<Nullable<UInt8>>(nullable_result_raw, "result");

    for (const auto * collator : collators)
    {
        auto nullable_result = executeFunction(func_ilike_name, {nullable_haystack, nullable_needle, escape}, collator);
        ASSERT_COLUMN_EQ(nullable_expected, nullable_result);
    }
}
CATCH

TEST_F(StringMatch, Ilike3ArgsConstantWithVector)
try
{
    struct Case
    {
        std::string src;
        std::vector<std::pair<std::string, int>> pat;
    };
    std::vector<Case> cases = {
        // {"a", {{"B", 0}, {"A", 1}, {"_", 1}, {"%", 1}}},
        {"aaB", {{"aAb", 1}, {"aB_", 0}, {"A_A", 0}, {"a__", 1}}},
    };

    for (const auto * collator : collators)
    {
        std::cout << "here\n";
        for (auto & cas : cases)
        {
            std::cout << "here11111\n";
            InferredDataVector<Nullable<String>> needle_raw = {};
            InferredDataVector<Nullable<UInt8>> result_raw = {};

            for (auto & pat : cas.pat)
            {
                needle_raw.push_back(pat.first);
                result_raw.push_back(pat.second);
            }

            auto haystack = createConstColumn<Nullable<String>>(1, cas.src);
            auto needle = createColumn<Nullable<String>>(needle_raw);
            auto expected = createColumn<Nullable<UInt8>>(result_raw);

            auto result = executeFunction(func_ilike_name, {haystack, needle, escape}, collator);
            ASSERT_COLUMN_EQ(expected, result);
        }
    }
}
CATCH

TEST_F(StringMatch, ilikeVectorWithVector)
{
    std::vector<std::optional<String>> haystack = {"我爱TiflaSH", "我爱TifLash", "", "A", "", "a", "a", "A", "ab", "aB", "a%", "aaAa", "aaaa", "aabaabABaabbab", "a", "abab", "abAB", "abcdefGHijklmn", "a", long_str};
    std::vector<std::optional<String>> needle = {"我_Tif%", "%爱tI%", "", "a", "", "%", "a%", "%a", "a%", "ab", "Ab", "a%", "aAab%", "aab%a%aab%b", "_", "_b__", "_b_", "a%", "abcDefghIjklmn%", long_pattern};
    std::vector<std::optional<UInt64>> expect = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1};

    std::vector<std::optional<String>> haystack_null = {{}, "a"};
    std::vector<std::optional<String>> needle_null = {"我_tif%", {}};
    std::vector<std::optional<UInt64>> expect_null = {{}, {}};

    for (const auto * collator : collators)
    {
        ASSERT_COLUMN_EQ(
            toNullableVec(expect),
            executeFunction(
                "ilike3Args",
                {toNullableVec(haystack), toNullableVec(needle), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect),
            executeFunction(
                "ilike3Args",
                {toVec(haystack), toVec(needle), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toNullableVec(expect_null),
            executeFunction(
                "ilike3Args",
                {toNullableVec(haystack_null), toNullableVec(needle_null), escape},
                collator));
    }
}

TEST_F(StringMatch, IlikeConstWithVector)
{
    std::vector<std::optional<String>> needle = {"", "a", "", "%", "a%", "%a", "a%", "ab", "ab", "a%", "aaab%", "aab%a%aab%b", "_", "_b__", "_b_", long_pattern};
    std::vector<std::optional<UInt64>> expect = {0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect1 = {0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1};

    std::vector<std::optional<String>> needle_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};

    for (const auto * collator : collators)
    {
        ASSERT_COLUMN_EQ(
            toNullableVec(expect),
            executeFunction(
                func_ilike_name,
                {toConst("abcAba"), toNullableVec(needle), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect),
            executeFunction(
                func_ilike_name,
                {toConst("ABCaba"), toVec(needle), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect1),
            executeFunction(
                func_ilike_name,
                {toConst(long_str), toVec(needle), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toNullableVec(expect_null),
            executeFunction(
                func_ilike_name,
                {toConst("ABC"), toNullableVec(needle_null), escape},
                collator));
    }
}

TEST_F(StringMatch, IlikeVectorWithConst)
{
    std::vector<std::optional<String>> haystack = {"我爱tiflash", "", "a", "", "a", "a", "A", "ab", "ab", "a%", "aaaa", "aaaa", "aabaABAbaabbaB", "a", "abab", "Abab", long_str};
    std::vector<std::optional<UInt64>> expect = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect1 = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    std::vector<std::optional<UInt64>> expect3 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};

    std::vector<std::optional<String>> haystack_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};

    for (const auto * collator : collators)
    {
        ASSERT_COLUMN_EQ(
            toNullableVec(expect),
            executeFunction(
                func_ilike_name,
                {toNullableVec(haystack), toConst("%aA%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect),
            executeFunction(
                func_ilike_name,
                {toVec(haystack), toConst("%aa%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect1),
            executeFunction(
                func_ilike_name,
                {toVec(haystack), toConst("%爱tIf%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect2),
            executeFunction(
                func_ilike_name,
                {toVec(haystack), toConst("%不爱tiF%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toVec(expect3),
            executeFunction(
                func_ilike_name,
                {toVec(haystack), toConst(long_pattern), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toNullableVec(expect_null),
            executeFunction(
                func_ilike_name,
                {toNullableVec(haystack_null), toConst("Abc"), escape},
                collator));
    }
}

TEST_F(StringMatch, IlikeConstWithConst)
{
    for (const auto * collator : collators)
    {
        ASSERT_COLUMN_EQ(
            toConst(1),
            executeFunction(
                func_ilike_name,
                {toConst("resaAsfe"), toConst("%aa%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toConst(0),
            executeFunction(
                func_ilike_name,
                {toConst("Abcde"), toConst("%aa%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toConst(1),
            executeFunction(
                func_ilike_name,
                {toConst("我爱Tiflash"), toConst("%爱tiF%"), escape},
                collator));

        ASSERT_COLUMN_EQ(
            toConst(0),
            executeFunction(
                func_ilike_name,
                {toConst("我爱tiflAsh"), toConst("%不爱tIf%"), escape},
                collator));
    }
}

TEST_F(StringMatch, ilike)
{
    String s;
    // using ColStringType = typename TypeTraits<String>::FieldType;
    // using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;
    ColumnWithTypeAndName escape = createConstColumn<Int32>(1, static_cast<Int32>('\\'));
    ColumnsWithTypeAndName data1{
        toVec(std::vector<std::optional<String>>(5, "aaa")),
        toVec(std::vector<std::optional<String>>(5, "aaa")),
        escape};
    // ColumnsWithTypeAndName data1{
    //     toVec<String>("col0", std::vector<ColStringType>(5, "aaaaaaaaaaaaaaaaa")),
    //     toVec<String>("col1", std::vector<ColStringType>(5, "aaaaaaaaaaaaaaaaa")),
    //     escape};
    FunctionIlike3Args function_ilike;
    TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
    function_ilike.setCollator(collator);
    std::vector<Block> blocks{Block(data1)};
    for (auto & block : blocks)
        block.insert({nullptr, std::make_shared<DataTypeNumber<UInt8>>(), "res"});
    ColumnNumbers arguments{0, 1, 2};
    for (auto & block : blocks)
        function_ilike.executeImpl(block, arguments, 3);
}

} // namespace tests
} // namespace DB

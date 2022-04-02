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

#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Storages/Transaction/Collator.h>
#include <TestUtils/FunctionTestUtils.h>

/// this is a hack, include the cpp file so we can test MatchImpl directly
#include <Functions/FunctionsStringSearch.cpp> // NOLINT
#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class Regexp : public FunctionTest
{
protected:
    static bool isColumnConstNull(const ColumnWithTypeAndName & column_with_type)
    {
        return column_with_type.column->isColumnConst() && column_with_type.column->isNullAt(0);
    }
    static bool isColumnConstNotNull(const ColumnWithTypeAndName & column_with_type)
    {
        return column_with_type.column->isColumnConst() && !column_with_type.column->isNullAt(0);
    }
    static bool isNullableColumnVector(const ColumnWithTypeAndName & column_with_type)
    {
        return !column_with_type.column->isColumnConst() && column_with_type.type->isNullable();
    }
    template <typename T>
    ColumnWithTypeAndName createNullableVectorColumn(const InferredDataVector<T> & vec, const std::vector<UInt8> & null_map)
    {
        using NullableType = Nullable<T>;
        InferredDataVector<NullableType> nullable_vec;
        for (size_t i = 0; i < null_map.size(); i++)
        {
            if (null_map[i])
                nullable_vec.push_back({});
            else
                nullable_vec.push_back(vec[i]);
        }
        return createColumn<NullableType>(nullable_vec);
    }
};

TEST_F(Regexp, testRegexpMatchType)
{
    UInt8 res = false;
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    const auto * ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "(?m)(?i)^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^b", '\\', "mi", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^b", '\\', "m", ci_collator, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^b", '\\', "mi", binary_collator, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^b", '\\', "i", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^b", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^a.*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^a.*B", '\\', "s", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB\n", "^a.*b", '\\', "is", nullptr, res);
    ASSERT_TRUE(res == 1);
}

TEST_F(Regexp, testRegexpMySQLFailedCases)
{
    UInt8 res = false;
    /// result different from mysql 8.x
    DB::MatchImpl<false, false, true>::constantConstant("aa", "((((((((((a))))))))))\\10", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AA", "(?i)((((((((((a))))))))))\\10", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)b\\s^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    /// back reference not supported in RE2
    // DB::MatchImpl<false, false, true>::constantConstant("abcabc", "(abc)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcabc", "([a-c]*)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a", "(a)|\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("x", "(a)|\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ababbbcbc", "(([a-c])b*?\\2)*", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ababbbcbc", "(([a-c])b*?\\2){3}", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaxabxbaxbbx", "((\\3|b)\\2(a)x)+", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaxabaxbaaxbbax", "((\\3|b)\\2(a)x)+", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("bbaababbabaaaaabbaaaabba", "((\\3|b)\\2(a)){2,}", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ABCABC", "(?i)(abc)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ABCABC", "(?i)([a-c]*)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("Ab4ab", "(?i)(ab)\\d\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ab4Ab", "(?i)(ab)\\d\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaaaa", "^(a\\1?)(a\\1?)(a\\2?)(a\\3?)$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abc", "^(?:b|a(?=(.)))*\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("Oo", "(?i)^(o)(?!.*\\1)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("abc12bc", "(.*)\\d+\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaab", "(?=(a+?))(\\1ab)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaab", "^(?=(a+?))\\1ab", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("2", "2(]*)?$\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcab", "(\\w)?(abc)\\1b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    /// invalid or unsupported Perl syntax: `(?!`
    // DB::MatchImpl<false, false, true>::constantConstant("abad", "a(?!b).", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid or unsupported Perl syntax: `(?=`
    // DB::MatchImpl<false, false, true>::constantConstant("abad", "a(?=c|d).", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abad", "a(?=d).", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=c)c", '\\', "", nullptr, res); /* Result: yB */
    // ;
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=b|c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=b|c)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=c|b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=c|b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=[bc])", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?=[bc])c", '\\', "", nullptr, res); /* Result: yB */
    // ;
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=c)c", '\\', "", nullptr, res); /* Result: yB */
    // ;
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=b|c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=b|c)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=c|b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=c|b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=[bc])", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?=[bc])c", '\\', "", nullptr, res); /* Result: yB */
    // ;
    /// invalid or unsupported Perl syntax: `(?<`
    // DB::MatchImpl<false, false, true>::constantConstant("ab", "(?<=a)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("cb", "(?<=a)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b", "(?<=a)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ab", "(?<!c)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("cb", "(?<!c)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b", "(?<!c)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("dbcb", "(?<![cd])b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("dbaacb", "(?<![cd])[ab]", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("dbcb", "(?<!(c|d))b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("dbaacb", "(?<!(c|d))[ab]", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("cdaccb", "(?<!cd)[ab]", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a", "$(?<=^(a))", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=b|c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=b|c)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=c|b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=c|b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=[bc])", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)(?<=[bc])c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=b|c)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=b|c)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=c|b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=c|b)c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=[bc])", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)(?<=[bc])c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid or unsupported Perl syntax: `(?#`
    // DB::MatchImpl<false, false, true>::constantConstant("aaac", "^a(?#xxx){3}c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaac", "(?x)^a (?#xxx) (?#yyy) {3}c", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid or unsupported Perl syntax: `(?s`
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "((?s).)c(?!.)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "((?s)b.)c(?!.)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid or unsupported Perl syntax: `(?>`
    // DB::MatchImpl<false, false, true>::constantConstant("aaab", "(?>a+)b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaab", "((?>a+)b)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aaab", "(?>(a+))b", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("((abc(ade)ufh()()x", "((?>[^()]+)|\\([^()]*\\))+", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("_I(round(xs * sz),1)", "round\\(((?>[^()]+))\\)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid escape sequence: `\Z`
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na", "\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na", "a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)a\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\naa", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\naa", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)aa\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nab", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nab", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)abb\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constantConstant("b\nac", "ab\\Z", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    /// invalid or unsupported Perl syntax: `(?x`
    // DB::MatchImpl<false, false, true>::constantConstant("x ", "(?x)((?x:.) )", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constantConstant("x ", "(?x)((?-x:.) )", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid or unsupported Perl syntax: `(?!`
    // DB::MatchImpl<false, false, true>::constantConstant("a\nxb\n", "(?m)(?!\\A)x", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid character class range: `a-[`
    // DB::MatchImpl<false, false, true>::constantConstant("za-9z", "([a-[:digit:]]+)", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    /// invalid escape sequence: `\G`
    // DB::MatchImpl<false, false, true>::constantConstant("aaaXbX", "\\GX.*X", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    /// invalid escape sequence: `\R`
    // DB::MatchImpl<false, false, true>::constantConstant("abc\n123\n456\nxyz\n", "(?m)^\\d+\\R\\d+$", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
}

TEST_F(Regexp, testRegexpMySQLCases)
{
    UInt8 res = false;
    // Test based on https://github.com/mysql/mysql-server/blob/mysql-cluster-8.0.17/mysql-test/t/regular_expressions_func.test
    DB::MatchImpl<false, false, true>::constantConstant("abc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xbc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("axc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abx", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("xabcy", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ababc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", ".{1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", ".{3,4}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab{0,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abq", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abq", "ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab{1,3}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab{3,4}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab{4,5}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abbc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab{0,1}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbbc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab{0,1}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abcc", "^abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aabc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aabc", "abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aabcd", "abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("axc", "a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("axyzc", "a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("axyzd", "a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abd", "a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abd", "a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ace", "a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aac", "a[b-d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-", "a[-b]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-", "a[b-]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_INVALID_RANGE
    // DB::MatchImpl<false,false,true>::constantConstant("-","a[b-a]",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constantConstant("-","a[]b",'\\',"",nullptr,res); /* Result: ci */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constantConstant("-","a[",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constantConstant("-","\\1",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constantConstant("-","\\2",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constantConstant("-","(a)|\\2",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("a]", "a]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a]b", "a[]]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aed", "a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abd", "a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("adc", "a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-c", "a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a]c", "a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("adc", "a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-a", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-a-", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xy", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("yz", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("xyz", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a-", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-a", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-a-", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("xy", "\\By\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("yz", "\\by\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xyz", "\\By\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "\\w", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-", "\\w", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a", "\\W", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-", "\\W", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a b", "a\\sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-b", "a\\sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a b", "a\\Sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a-b", "a\\Sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("1", "\\d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-", "\\d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("1", "\\D", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-", "\\D", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "[\\w]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-", "[\\w]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a", "[\\W]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-", "[\\W]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a b", "a[\\s]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a-b", "a[\\s]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a b", "a[\\S]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a-b", "a[\\S]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("1", "[\\d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("-", "[\\d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("1", "[\\D]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("-", "[\\D]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("def", "()ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","*a",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","(*)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("b", "$b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // error ER_REGEXP_BAD_ESCAPE_SEQUENCE
    // DB::MatchImpl<false,false,true>::constantConstant("-","a\\",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("a(b", "a\\(b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a((b", "a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\\b", "a\\\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-","abc)",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-","(abc",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("abc", "((a))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "(a)b(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aabbabc", "a+b+c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aabbabc", "a{1,}b{1,}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","a**",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("abcabc", "a.+?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b)*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b){0,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b){1,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(a+|b){0,1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-",",'\\',"",nullptr,res);(",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("cde", "[^ab]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("", "a*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abbbcd", "([abc])*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "([abc])*bcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("e", "a|b|c|d|e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ef", "(a|b|c|d|e)f", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcdefg", "abcd*efg", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xabyabbbz", "ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xayabbbz", "ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcde", "(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("hij", "[abhgefdc]ij", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcde", "^(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abcdef", "(abc|)ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "(a|b)c*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "(ab|ab*)bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "a([bc]*)c*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "a([bc]*)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "a([bc]+)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "a([bc]*)(c+d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("adcdcde", "a[bcd]*dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("adcdcde", "a[bcd]+dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "(ab|a)b*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "((a)(b)c)(d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("alpha", "[a-zA-Z_][a-zA-Z0-9_]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abh", "^a(bc+|b[eh])g|.h$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("effgz", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ij", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("effg", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("bcdd", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("reffgz", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "((((((((((a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "(((((((((a)))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("uh-uh", "multiple words of text", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("multiple words, yeah", "multiple words", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcde", "(.*)c(.*)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("(a, b)", "\\((.*), (.*)\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "[k]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "abcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "a(bc)d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ac", "a[-]?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b", "(a)|(b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("XBC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AXC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABX", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("XABCY", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABABC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBC", "(?i)ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab*?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab{0,}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBC", "(?i)ab+?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABQ", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABQ", "(?i)ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab{1,}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab{1,3}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab{3,4}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab{4,5}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABBC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab{0,1}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBBC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab??c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab{0,1}?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABCC", "(?i)^abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AABC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AABC", "(?i)abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AXC", "(?i)a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AXYZC", "(?i)a.*?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AXYZD", "(?i)a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABD", "(?i)a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABD", "(?i)a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ACE", "(?i)a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AAC", "(?i)a[b-d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A-", "(?i)a[-b]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A-", "(?i)a[b-]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_INVALID_RANGE
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)a[b-a]",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)a[]b",'\\',"",nullptr,res); /* Result: ci */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)a[",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("A]", "(?i)a]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A]B", "(?i)a[]]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AED", "(?i)a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABD", "(?i)a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ADC", "(?i)a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A-C", "(?i)a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("A]C", "(?i)a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ADC", "(?i)a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("DEF", "(?i)()ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)*a",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)(*)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("B", "(?i)$b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // error ER_REGEXP_BAD_ESCAPE_SEQUENCE
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)a\\",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("A(B", "(?i)a\\(b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A((B", "(?i)a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A\\B", "(?i)a\\\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)abc)",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)(abc",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)((a))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)(a)b(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AABBABC", "(?i)a+b+c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AABBABC", "(?i)a{1,}b{1,}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i)a**",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("ABCABC", "(?i)a.+?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCABC", "(?i)a.*?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCABC", "(?i)a.{0,5}?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b)*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b){0,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b){1,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b){0,1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(a+|b){0,1}?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?i))(",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("CDE", "(?i)[^ab]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("", "(?i)a*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABBBCD", "(?i)([abc])*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)([abc])*bcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("E", "(?i)a|b|c|d|e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("EF", "(?i)(a|b|c|d|e)f", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCDEFG", "(?i)abcd*efg", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("XABYABBBZ", "(?i)ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("XAYABBBZ", "(?i)ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCDE", "(?i)(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("HIJ", "(?i)[abhgefdc]ij", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCDE", "(?i)^(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABCDEF", "(?i)(abc|)ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)(a|b)c*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)(ab|ab*)bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)a([bc]*)c*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)a([bc]*)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)a([bc]+)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)a([bc]*)(c+d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ADCDCDE", "(?i)a[bcd]*dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ADCDCDE", "(?i)a[bcd]+dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABC", "(?i)(ab|a)b*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)((a)(b)c)(d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ALPHA", "(?i)[a-zA-Z_][a-zA-Z0-9_]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABH", "(?i)^a(bc+|b[eh])g|.h$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("EFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("IJ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("EFFG", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("BCDD", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("REFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A", "(?i)((((((((((a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A", "(?i)(((((((((a)))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("C", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a|b|c))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("UH-UH", "(?i)multiple words of text", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("MULTIPLE WORDS, YEAH", "(?i)multiple words", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCDE", "(?i)(.*)c(.*)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("(A, B)", "(?i)\\((.*), (.*)\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)[k]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)abcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ABCD", "(?i)a(bc)d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AC", "(?i)a[-]?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ace", "a(?:b|c|d)(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ace", "a(?:b|c|d)*(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ace", "a(?:b|c|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d)+(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){2}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){4,5}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){4,5}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("foobar", "((foo)|(bar))*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constantConstant("-",":(?:",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){6,7}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){6,7}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){5,6}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){5,6}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){5,7}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("acdbcdbe", "a(?:b|c|d){5,7}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ace", "a(?:b|(c|e){1,2}?|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "^(.+)?B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant(".", "^([^a-z])|(\\^)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("<&OUT", "^[<>]&", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("aaaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("aaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("aaaaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaa", "((a{4})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaaa", "(((aa){2})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaaaaaaaaa", "(((a{2}){2})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("foobar", "(?:(f)(o)(o)|(b)(a)(r))*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?<%)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("aba", "(?:..)*a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aba", "(?:..)*?a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "^(){3,5}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aax", "^(a+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aax", "^((a|b)+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aax", "^((a|bc)+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("cab", "(a|x)*ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("cab", "(a)*ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aB", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("Ab", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("AB", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB", "(?i)((?-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nB", "(?i)((?s-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("B\nB", "(?i)((?s-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant(
        "cabbbb",
        "(?:c|d)(?:)(?:a(?:)(?:b)(?:b(?:))(?:b(?:)(?:b)))",
        '\\',
        "",
        nullptr,
        res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("caaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                                        "(?:c|d)(?:)(?:aaaaaaaa(?:)(?:bbbbbbbb)(?:bbbbbbbb(?:))(?:bbbbbbbb(?:)(?:bbbbbbbb)))",
                                                        '\\',
                                                        "",
                                                        nullptr,
                                                        res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("foobar1234baz", "foo\\w*\\d{4}baz", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("cabd","a(?{})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("-","a(?{)b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("-","a(?{{})b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("-","a(?{}})b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("-","a(?{"{"})b",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("cabd","a(?{"\\{"})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("-","a(?{"{"}})b",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("caxbd","a(?{$bl="\\{"}).b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("x~~", "x(~~)*(?:(?:F)?)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a--", "^(?:a?b?)*$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "((?s)^a(.))((?m)^b$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "((?m)^b$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)^(b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "\n((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "()^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\nc\n", "((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(x)?(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(x)?(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","()?(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","()(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","()?(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("(blah)","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("blah","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("blah)","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("(blah","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("(blah)","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("blah","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("blah)","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("(blah","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(1?)a|b)",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(1)a|b|c)",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?{0})a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?{0})b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?{1})b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?{1})a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?!a)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?!a)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?=a)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","(?(?=a)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("one:", "(\\w+:)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd:", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xy:z:::abcd", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aexycd", "^[^bcd]*(c+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("caab", "(a*)b+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("yaaxxaaaacd","(?{$a=2})a*aa(?{local$a=$a+1})k*c(?{$b=$a})",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("yaaxxaaaacd","(?{$a=2})(a(?{local$a=$a+1}))*aak*c(?{$b=$a})",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaab", "(>a+)ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a:[b]:", "([\\[:]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a=[b]=", "([\\[=]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a.[b].", "([\\[.]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constantConstant("-","[a[:xyz:",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constantConstant("-","[a[:xyz:]",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("abc", "[a\\[:]b[:c]", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constantConstant("pbaq","([a[:xyz:]b]+)",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("abc", "[a\\[:]b[:c]", '\\', "", nullptr, res); /* Result: iy */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constantConstant("-","[[:foo:]]",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constantConstant("-","[[:^foo:]]",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_LOOK_BEHIND_LIMIT
    // DB::MatchImpl<false,false,true>::constantConstant("-","(?<=x+)y",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_MAX_LT_MIN
    // DB::MatchImpl<false,false,true>::constantConstant("-","a{37,17}",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a\nb\n", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\na\n", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\na", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aa\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\naa", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ab\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nab", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abb\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b\nabb", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ac\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nac", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b\nca", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("ca", "(^|x)(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("x", "a*abc?xyz+pqr{3}ab{2,}xy{4,5}pq{0,6}AB{0,}zz", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("yabz","a(?{$a=2;$b=3;($b)=$a})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("foo.bart", "foo.bart", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd\ndxxx", "(?m)^d[x][x][x]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("xxxtt", "tt+$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("za-9z", "([a\\-\\d]+)", '\\', "", nullptr, res); /* Result: yi */
    ;
    DB::MatchImpl<false, false, true>::constantConstant("a0-za", "([\\d-z]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a0- z", "([\\d-\\s]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("=0-z=", "([[:digit:]-z]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("=0-z=", "([[:digit:]-[:alpha:]]+)", '\\', "", nullptr, res); /* Result: iy */
    ;
    DB::MatchImpl<false, false, true>::constantConstant("3.1415926", R"((\d+\.\d+))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("have a web browser", "(\\ba.{0,10}br)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("Changes", "(?i)\\.c(pp|xx|c)?$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("IO.c", "(?i)\\.c(pp|xx|c)?$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("IO.c", "(?i)(\\.c(pp|xx|c)?$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("C:/", "^([a-z]:)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("\nx aa", "(?m)^\\S\\s+aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(^|a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abac", "^([ab]*?)(b)?(c)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a,b,c", "^(?:.,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a,b,c", "^(.,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a,b,c", "^(?:[^,]*,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a,b,c", "^([^,]*,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]*,){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]*,){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]*,){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,3},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,3},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,3},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{1,},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{0,3},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{0,3},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaa,b,c,d", "^([^,]{0,3},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("", "(?i)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aba", "^(a(b)?)+$", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("123\nabcabcabcabc\n", "(?m)^.{9}abc.*\n", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "^(a)?a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("a","^(a)?(?(1)a|b)+$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("x1", "^(0+)?(?:x(1))?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant(
        "012cxx0190",
        "^([0-9a-fA-F]+)(?:x([0-9a-fA-F]+)?)(?:x([0-9a-fA-F]+))?",
        '\\',
        "",
        nullptr,
        res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("bbbac", "^(b+?|a){1,2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("bbbbac", "^(b+?|a){1,2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("cd. (A. Tw)", R"(\((\w\. \w+)\))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("aaaacccc", "((?:aaaa|bbbb)cccc)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("bbbbcccc", "((?:aaaa|bbbb)cccc)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "(a)?(a)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "(ab)?(ab)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "(abc)?(abc)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", "\\ba", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # ?? Not supported
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("ab","^(a(??{"(?!)"})|(a)(?{1}))b",'\\',"",nullptr,res); /* Result: yi */;
    DB::MatchImpl<false, false, true>::constantConstant("AbCd", "ab(?i)cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("abCd", "ab(?i)cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("CD","(A|B)*(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("ABCD","(A|B)*(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("CD","(A|B)*?(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constantConstant("ABCD","(A|B)*?(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("foo\n bar", "(?m:(foo\\s*$))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", "(.*?)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # ?? not supported
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constantConstant("x","(??{})",'\\',"",nullptr,res); /* Result: yi */;
    DB::MatchImpl<false, false, true>::constantConstant("abc", "a", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "b", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "c", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "d", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a", "a.*", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "a.*", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "A", '\\', "i", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abc", "A", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
}

TEST_F(Regexp, testRegexpTiDBCase)
{
    UInt8 res;
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    const auto * ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    DB::MatchImpl<false, false, true>::constantConstant("a", "^$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("a", "a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("b", "a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aA", "aA", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("A", "^a$", '\\', "", binary_collator, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("A", "^a$", '\\', "", ci_collator, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("a", ".", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("ab", "^.$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("b", "..", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("aab", ".ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("abcd", ".*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("测试", "^.$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constantConstant("测", "^.$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constantConstant("平凯星辰", "^平..辰$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    ASSERT_ANY_THROW((DB::MatchImpl<false, false, true>::constantConstant("", "(", '\\', "", nullptr, res)));
    ASSERT_ANY_THROW((DB::MatchImpl<false, false, true>::constantConstant("", "(*", '\\', "", nullptr, res)));
    ASSERT_ANY_THROW((DB::MatchImpl<false, false, true>::constantConstant("", "[a", '\\', "", nullptr, res)));
    ASSERT_ANY_THROW((DB::MatchImpl<false, false, true>::constantConstant("", "\\", '\\', "", nullptr, res)));
}

TEST_F(Regexp, testRegexp)
{
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_string_type = makeNullable(string_type);
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto nullable_uint8_type = makeNullable(uint8_type);

    std::vector<String> input_strings{"abc", "Abc", "a\nb\nc", "abcd", "hello, 平凯星辰"};
    std::vector<UInt8> input_string_nulls{0, 0, 0, 1, 0};

    std::vector<String> patterns{"^a", "abc$", "a.*B.*c", "^bc$", "平凯.*"};
    std::vector<UInt8> pattern_nulls{1, 0, 0, 0, 0};

    std::vector<String> match_types{"", "i", "ims", "i", ""};
    std::vector<UInt8> match_type_nulls{0, 1, 0, 0, 0};

    std::vector<UInt64> results{1, 0, 0, 0, 1};
    std::vector<UInt64> results_with_match_type{1, 1, 1, 0, 1};
    std::vector<UInt64> results_with_match_type_collator{1, 0, 0, 0, 1};

    std::vector<UInt64> vec_results{1, 0, 1, 1, 0};
    std::vector<UInt64> vec_results_with_match_type{1, 1, 1, 1, 0};
    std::vector<UInt64> vec_results_with_match_type_collator{1, 0, 1, 1, 0};

    size_t row_size = input_string_nulls.size();

    auto const_uint8_null_column = createConstColumn<Nullable<UInt8>>(row_size, {});
    auto const_string_null_column = createConstColumn<Nullable<String>>(row_size, {});
    /// case 1. regexp(const, const [, const])
    for (size_t i = 0; i < row_size; i++)
    {
        /// test regexp(const, const)
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(row_size, results[i]),
                         executeFunction("regexp", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i])));

        /// test regexp(const, const, const)
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(row_size, results_with_match_type[i]),
                         executeFunction("regexp", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, match_types[i])));

        /// test regexp(const, const, const) with binary collator
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(row_size, results_with_match_type_collator[i]),
                         executeFunction("regexp", {createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, match_types[i])}, binary_collator));
    }
    /// case 2. regexp(const, const [, const]) with null value
    for (size_t i = 0; i < row_size; i++)
    {
        /// test regexp(const, const)
        ASSERT_COLUMN_EQ(input_string_nulls[i] || pattern_nulls[i] ? const_uint8_null_column : createConstColumn<Nullable<UInt8>>(row_size, results[i]),
                         executeFunction("regexp", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i])));

        /// test regexp(const, const, const)
        ASSERT_COLUMN_EQ(input_string_nulls[i] || pattern_nulls[i] || match_type_nulls[i] ? const_uint8_null_column : createConstColumn<Nullable<UInt8>>(row_size, results_with_match_type[i]),
                         executeFunction("regexp", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), match_type_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, match_types[i])));

        /// test regexp(const, const, const) with binary collator
        ASSERT_COLUMN_EQ(input_string_nulls[i] || pattern_nulls[i] || match_type_nulls[i] ? const_uint8_null_column : createConstColumn<Nullable<UInt8>>(row_size, results_with_match_type_collator[i]),
                         executeFunction("regexp", {input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), match_type_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, match_types[i])}, binary_collator));
    }
    /// case 3 regexp(vector, const[, const])
    {
        /// test regexp(vector, const)
        ASSERT_COLUMN_EQ(createColumn<UInt8>(vec_results),
                         executeFunction("regexp", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0])));

        /// test regexp(vector, const, const)
        ASSERT_COLUMN_EQ(createColumn<UInt8>(vec_results_with_match_type),
                         executeFunction("regexp", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, "i")));

        /// test regexp(vector, const, const) with binary collator
        ASSERT_COLUMN_EQ(createColumn<UInt8>(vec_results_with_match_type_collator),
                         executeFunction("regexp", {createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, "i")}, binary_collator));
    }
    /// case 4 regexp(vector, const[, const]) nullable
    {
        ASSERT_COLUMN_EQ(createNullableVectorColumn<UInt8>(vec_results, input_string_nulls),
                         executeFunction("regexp", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0])));
        ASSERT_COLUMN_EQ(createNullableVectorColumn<UInt8>(vec_results_with_match_type, input_string_nulls),
                         executeFunction("regexp", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, "i")));
        ASSERT_COLUMN_EQ(createNullableVectorColumn<UInt8>(vec_results_with_match_type_collator, input_string_nulls),
                         executeFunction("regexp", {createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, "i")}, binary_collator));
    }
}

TEST_F(Regexp, testRegexpCustomerCases)
{
    String pattern = "^(53|94)[0-9]{10}$|"
                     "^(1200|1201|1202|1203|1204|1205|1206|1207|1208)[0-9]{8}$|"
                     "^54[0-9]{10}$|"
                     "^665[0-9]{9}$|"
                     "^63[0-9]{10}$|"
                     "^731[0-9]{11}$|"
                     "^73220[0-9]{9}$|"
                     "^73200[0-9]{9}$|"
                     "^73210[0-9]{9}$|"
                     "^771[0-9]{11}$|"
                     "^91[0-9]{10}$|"
                     "^73211[0-9]{9}$|"
                     "^781[0-9]{11}$|"
                     "^73222[0-9]{9}$|"
                     "^734[0-9]{11}$|"
                     "^75210[0-9]{9}$|"
                     "^73223[0-9]{9}$|"
                     "^73224[0-9]{9}$|"
                     "^882[0-9]{9}$|"
                     "^7777[0-9]{10}$|"
                     "^758[0-9]{11}$|"
                     "^759[0-9]{11}$|"
                     "^73226[0-9]{9}$|"
                     "^77761[0-9]{9}$|"
                     "^73227[0-9]{9}$|"
                     "^73225[0-9]{9}$|"
                     "^31111[0-9]{9}$|"
                     "^754[0-9]{11}$|"
                     "^755[0-9]{11}$|"
                     "^73228[0-9]{9}$|"
                     "^73229[0-9]{9}$|"
                     "^782[0-9]{11}$|"
                     "^756[0-9]{11}$";
    std::vector<String> patterns{pattern, pattern, pattern, pattern, pattern};
    std::vector<String> inputs{"73228012343218", "530101343498", "540101323298", "31111191919191", "78200000000000"};
    /// columnNothing, columnConstNull, columnConstNotNull, columnVectorNullable, columnVectorNotNull
    ColumnsWithTypeAndName input_columns{createOnlyNullColumnConst(5), createConstColumn<Nullable<String>>(5, {}), createConstColumn<Nullable<String>>(5, inputs[0]), createConstColumn<String>(5, inputs[0]), createColumn<Nullable<String>>({inputs[0], {}, {}, inputs[3], inputs[4]}), createColumn<String>(inputs)};
    ColumnsWithTypeAndName pattern_columns{createOnlyNullColumnConst(5), createConstColumn<Nullable<String>>(5, {}), createConstColumn<Nullable<String>>(5, patterns[0]), createConstColumn<String>(5, patterns[0]), createColumn<Nullable<String>>({patterns[0], {}, {}, patterns[3], patterns[4]}), createColumn<String>(patterns)};
    for (const auto & input_column : input_columns)
    {
        for (const auto & pattern_column : pattern_columns)
        {
            if (input_column.type->onlyNull() || pattern_column.type->onlyNull())
            {
                ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5),
                                 executeFunction("regexp", input_column, pattern_column));
            }
            else if (isColumnConstNull(input_column) || isColumnConstNull(pattern_column))
            {
                ASSERT_COLUMN_EQ(createConstColumn<Nullable<UInt8>>(5, {}),
                                 executeFunction("regexp", input_column, pattern_column));
            }
            else if (isColumnConstNotNull(input_column) && isColumnConstNotNull(pattern_column))
            {
                if (input_column.type->isNullable() || pattern_column.type->isNullable())
                {
                    ASSERT_COLUMN_EQ(createConstColumn<Nullable<UInt8>>(5, 1),
                                     executeFunction("regexp", input_column, pattern_column));
                }
                else
                {
                    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(5, 1),
                                     executeFunction("regexp", input_column, pattern_column));
                }
            }
            else
            {
                bool result_nullable = input_column.type->isNullable() || pattern_column.type->isNullable();
                if (!result_nullable)
                {
                    ASSERT_COLUMN_EQ(createColumn<UInt8>({1, 1, 1, 1, 1}),
                                     executeFunction("regexp", input_column, pattern_column));
                }
                else
                {
                    bool input_contains_null = isNullableColumnVector(input_column) || isNullableColumnVector(pattern_column);
                    if (input_contains_null)
                    {
                        ASSERT_COLUMN_EQ(createColumn<Nullable<UInt8>>({1, {}, {}, 1, 1}),
                                         executeFunction("regexp", input_column, pattern_column));
                    }
                    else
                    {
                        ASSERT_COLUMN_EQ(createColumn<Nullable<UInt8>>({1, 1, 1, 1, 1}),
                                         executeFunction("regexp", input_column, pattern_column));
                    }
                }
            }
        }
    }
}

TEST_F(Regexp, testRegexpReplaceMatchType)
{
    String res;
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    const auto * ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "(?m)(?i)^b", "xxx", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "a\nxxx\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "^b", "xxx", 1, 0, "mi", nullptr, res);
    ASSERT_TRUE(res == "a\nxxx\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "^b", "xxx", 1, 0, "m", ci_collator, res);
    ASSERT_TRUE(res == "a\nxxx\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "^b", "xxx", 1, 0, "mi", binary_collator, res);
    ASSERT_TRUE(res == "a\nB\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "^b", "xxx", 1, 0, "i", nullptr, res);
    ASSERT_TRUE(res == "a\nB\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\nc", "^b", "xxx", 1, 0, "m", nullptr, res);
    ASSERT_TRUE(res == "a\nB\nc");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\n", "^a.*b", "xxx", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "a\nB\n");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\n", "^a.*B", "xxx", 1, 0, "s", nullptr, res);
    ASSERT_TRUE(res == "xxx\n");
    DB::ReplaceRegexpImpl<false>::constant("a\nB\n", "^a.*b", "xxx", 1, 0, "is", nullptr, res);
    ASSERT_TRUE(res == "xxx\n");
}

TEST_F(Regexp, testRegexpReplaceMySQLCases)
{
    // Test based on https://github.com/mysql/mysql-server/blob/mysql-cluster-8.0.17/mysql-test/t/regular_expressions_utf-8.test
    String res;
    DB::ReplaceRegexpImpl<false>::constant("aaa", "a", "X", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "XXX");
    DB::ReplaceRegexpImpl<false>::constant("abc", "b", "X", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aXc");
    DB::ReplaceRegexpImpl<false>::constant("aaabbccbbddaa", "b+", "X", 1, 1, "", nullptr, res);
    ASSERT_TRUE(res == "aaaXccbbddaa");
    DB::ReplaceRegexpImpl<false>::constant("aaabbccbbddaa", "b+", "X", 1, 2, "", nullptr, res);
    ASSERT_TRUE(res == "aaabbccXddaa");
    DB::ReplaceRegexpImpl<false>::constant("aaabbccbbddaa", "(b+)", "<\\1>", 1, 2, "", nullptr, res);
    ASSERT_TRUE(res == "aaabbcc<bb>ddaa");
    DB::ReplaceRegexpImpl<false>::constant("aaabbccbbddaa", "x+", "x", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aaabbccbbddaa");
    DB::ReplaceRegexpImpl<false>::constant("aaabbccbbddaa", "b+", "x", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aaaxccxddaa");
    DB::ReplaceRegexpImpl<false>::constant("aaab", "b", "x", 1, 2, "", nullptr, res);
    ASSERT_TRUE(res == "aaab");
    DB::ReplaceRegexpImpl<false>::constant("aaabccc", "b", "x", 1, 2, "", nullptr, res);
    ASSERT_TRUE(res == "aaabccc");
    DB::ReplaceRegexpImpl<false>::constant("abcbdb", "b", "X", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aXcXdX");
    DB::ReplaceRegexpImpl<false>::constant("aaabcbdb", "b", "X", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aaaXcXdX");
    DB::ReplaceRegexpImpl<false>::constant("aaabcbdb", "b", "X", 2, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aaaXcXdX");
    DB::ReplaceRegexpImpl<false>::constant("aaabcbdb", "b", "X", 3, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aaaXcXdX");
    DB::ReplaceRegexpImpl<false>::constant("aaa", "a", "X", 2, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aXX");
    DB::ReplaceRegexpImpl<false>::constant("aaa", "a", "XX", 2, 0, "", nullptr, res);
    ASSERT_TRUE(res == "aXXXX");
    DB::ReplaceRegexpImpl<false>::constant("c b b", "^([[:alpha:]]+)[[:space:]].*$", "\\1", 1, 0, "", nullptr, res);
    ASSERT_TRUE(res == "c");
    DB::ReplaceRegexpImpl<false>::constant("\U0001F450\U0001F450\U0001F450", ".", "a", 2, 0, "", nullptr, res);
    ASSERT_TRUE(res == "\U0001F450aa");
    DB::ReplaceRegexpImpl<false>::constant("\U0001F450\U0001F450\U0001F450", ".", "a", 2, 2, "", nullptr, res);
    ASSERT_TRUE(res == "\U0001F450\U0001F450a");
}

TEST_F(Regexp, testRegexpReplace)
{
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_string_type = makeNullable(string_type);
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto nullable_uint8_type = makeNullable(uint8_type);

    std::vector<String> input_strings{"abb\nabbabb", "abbcabbabb", "abbabbabb", "ABBABBABB", "ABB\nABBABB"};
    std::vector<UInt8> input_string_nulls{0, 1, 0, 0, 0};

    std::vector<String> patterns{"^a.*", "bb", "abc", "abb", "abb.abb"};
    std::vector<UInt8> pattern_nulls{0, 0, 1, 0, 0};

    std::vector<String> replacements{"xxx", "xxx", "xxx", "xxx", "xxx"};
    std::vector<UInt8> replacement_nulls{0, 0, 1, 0, 0};

    std::vector<Int64> pos{1, 3, 2, 2, 1};
    std::vector<UInt8> pos_nulls{0, 0, 0, 1, 0};

    std::vector<Int64> occ{0, 2, 0, 0, 0};
    std::vector<UInt8> occ_nulls{1, 0, 0, 0, 0};

    std::vector<String> match_types{"is", "", "", "i", "ism"};
    std::vector<UInt8> match_type_nulls{1, 0, 0, 0, 0};

    std::vector<String> results{"xxx\nabbabb", "axxxcaxxxaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> results_with_pos{"xxx\nabbabb", "abbcaxxxaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> results_with_pos_occ{"xxx\nabbabb", "abbcabbaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> results_with_pos_occ_match_type{"xxx", "abbcabbaxxx", "abbabbabb", "ABBxxxxxx", "xxxABB"};
    std::vector<String> results_with_pos_occ_match_type_binary{"xxx", "abbcabbaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB"};

    std::vector<String> vec_results{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> vec_results_with_pos{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> vec_results_with_pos_occ{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB"};
    std::vector<String> vec_results_with_pos_occ_match_type{"xxx", "xxx", "xxx", "xxx", "xxx"};
    std::vector<String> vec_results_with_pos_occ_match_type_binary{"xxx", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB"};

    size_t row_size = input_strings.size();
    auto const_string_null_column = createConstColumn<Nullable<String>>(row_size, {});
    auto const_int64_null_column = createConstColumn<Nullable<Int64>>(row_size, {});

    /// case 1. regexp_replace(const, const, const [, const, const ,const])
    for (size_t i = 0; i < match_types.size(); i++)
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(createConstColumn<String>(row_size, results[i]),
                         executeFunction("replaceRegexpAll", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, replacements[i])));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(createConstColumn<String>(row_size, results_with_pos[i]),
                         executeFunction("replaceRegexpAll", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, replacements[i]), createConstColumn<Int64>(row_size, pos[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(createConstColumn<String>(row_size, results_with_pos_occ[i]),
                         executeFunction("replaceRegexpAll", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, replacements[i]), createConstColumn<Int64>(row_size, pos[i]), createConstColumn<Int64>(row_size, occ[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(createConstColumn<String>(row_size, results_with_pos_occ_match_type[i]),
                         executeFunction("replaceRegexpAll", createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, replacements[i]), createConstColumn<Int64>(row_size, pos[i]), createConstColumn<Int64>(row_size, occ[i]), createConstColumn<String>(row_size, match_types[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(createConstColumn<String>(row_size, results_with_pos_occ_match_type_binary[i]),
                         executeFunction("replaceRegexpAll", {createConstColumn<String>(row_size, input_strings[i]), createConstColumn<String>(row_size, patterns[i]), createConstColumn<String>(row_size, replacements[i]), createConstColumn<Int64>(row_size, pos[i]), createConstColumn<Int64>(row_size, occ[i]), createConstColumn<String>(row_size, match_types[i])}, binary_collator));
    }

    /// case 2. regexp_replace(const, const, const [, const, const ,const]) with null value
    for (size_t i = 0; i < match_types.size(); i++)
    {
        /// test regexp_replace(str, pattern, replacement)
        bool null_result = input_string_nulls[i] || pattern_nulls[i] || replacement_nulls[i];
        ASSERT_COLUMN_EQ(null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results[i]),
                         executeFunction("replaceRegexpAll", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), replacement_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, replacements[i])));

        /// test regexp_replace(str, pattern, replacement, pos)
        null_result = null_result || pos_nulls[i];
        ASSERT_COLUMN_EQ(null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results_with_pos[i]),
                         executeFunction("replaceRegexpAll", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), replacement_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, replacements[i]), pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        null_result = null_result || occ_nulls[i];
        ASSERT_COLUMN_EQ(null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ[i]),
                         executeFunction("replaceRegexpAll", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), replacement_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, replacements[i]), pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]), occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        null_result = null_result || match_type_nulls[i];
        ASSERT_COLUMN_EQ(null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ_match_type[i]),
                         executeFunction("replaceRegexpAll", input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), replacement_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, replacements[i]), pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]), occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i]), match_type_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, match_types[i])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ_match_type_binary[i]),
                         executeFunction("replaceRegexpAll", {input_string_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, input_strings[i]), pattern_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, patterns[i]), replacement_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, replacements[i]), pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]), occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i]), match_type_nulls[i] ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, match_types[i])}, binary_collator));
    }

    /// case 3 regexp_replace(vector, const, const[, const, const, const])
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(createColumn<String>(vec_results),
                         executeFunction("replaceRegexpAll", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0])));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(createColumn<String>(vec_results_with_pos),
                         executeFunction("replaceRegexpAll", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0])));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(createColumn<String>(vec_results_with_pos_occ),
                         executeFunction("replaceRegexpAll", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(createColumn<String>(vec_results_with_pos_occ_match_type),
                         executeFunction("replaceRegexpAll", createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0]), createConstColumn<String>(row_size, match_types[0])));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(createColumn<String>(vec_results_with_pos_occ_match_type_binary),
                         executeFunction("replaceRegexpAll", {createColumn<String>(input_strings), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0]), createConstColumn<String>(row_size, match_types[0])}, binary_collator));
    }

    /// case 4 regexp_replace(vector, const, const[, const, const, const]) with null value
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(createNullableVectorColumn<String>(vec_results, input_string_nulls),
                         executeFunction("replaceRegexpAll", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0])));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(createNullableVectorColumn<String>(vec_results_with_pos, input_string_nulls),
                         executeFunction("replaceRegexpAll", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0])));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(createNullableVectorColumn<String>(vec_results_with_pos_occ, input_string_nulls),
                         executeFunction("replaceRegexpAll", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0])));


        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(createNullableVectorColumn<String>(vec_results_with_pos_occ_match_type, input_string_nulls),
                         executeFunction("replaceRegexpAll", createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0]), createConstColumn<String>(row_size, match_types[0])));


        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(createNullableVectorColumn<String>(vec_results_with_pos_occ_match_type_binary, input_string_nulls),
                         executeFunction("replaceRegexpAll", {createNullableVectorColumn<String>(input_strings, input_string_nulls), createConstColumn<String>(row_size, patterns[0]), createConstColumn<String>(row_size, replacements[0]), createConstColumn<Int64>(row_size, pos[0]), createConstColumn<Int64>(row_size, occ[0]), createConstColumn<String>(row_size, match_types[0])}, binary_collator));
    }
}
} // namespace tests
} // namespace DB
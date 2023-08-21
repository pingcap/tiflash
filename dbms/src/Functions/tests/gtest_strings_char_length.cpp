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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
namespace DB
{
namespace tests
{
class StringCharLength : public FunctionTest
{
protected:
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<UInt64>> & v)
    {
        return createColumn<Nullable<UInt64>>(v);
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
        return createColumn<UInt64>(ints);
    }

    static ColumnWithTypeAndName toConst(const String & s) { return createConstColumn<String>(1, s); }

    static ColumnWithTypeAndName toConst(const UInt64 i) { return createConstColumn<UInt64>(1, i); }
};

TEST_F(StringCharLength, charLengthVector)
{
    std::vector<std::optional<String>> candidate_strings
        = {"", "a", "do you know the length?", "你知道字符串的长度吗？", "你知道字符串的 length 吗?？"};
    std::vector<std::optional<UInt64>> expect = {0, 1, 23, 11, 18};
    ASSERT_COLUMN_EQ(toNullableVec(expect), executeFunction("lengthUTF8", toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(toVec(expect), executeFunction("lengthUTF8", toVec(candidate_strings)));

    std::vector<std::optional<String>> candidate_strings_null = {{}};
    std::vector<std::optional<UInt64>> expect_null = {{}};
    ASSERT_COLUMN_EQ(toNullableVec(expect_null), executeFunction("lengthUTF8", toNullableVec(candidate_strings_null)));
}

TEST_F(StringCharLength, charLengthConst)
{
    ASSERT_COLUMN_EQ(toConst(0), executeFunction("lengthUTF8", toConst("")));

    ASSERT_COLUMN_EQ(toConst(23), executeFunction("lengthUTF8", toConst("do you know the length?")));

    ASSERT_COLUMN_EQ(toConst(11), executeFunction("lengthUTF8", toConst("你知道字符串的长度吗？")));

    ASSERT_COLUMN_EQ(toConst(18), executeFunction("lengthUTF8", toConst("你知道字符串的 length 吗?？")));
}

} // namespace tests
} // namespace DB

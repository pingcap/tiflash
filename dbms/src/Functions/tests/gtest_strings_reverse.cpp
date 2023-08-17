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
#include <Functions/FunctionsString.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic pop

namespace DB::tests
{
class StringReverse : public DB::tests::FunctionTest
{
protected:
    static ColumnWithTypeAndName toVec(const std::vector<String> & v) { return createColumn<String>(v); }

    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toConst(const String & s) { return createConstColumn<String>(1, s); }
};
// test reverse
TEST_F(StringReverse, stringReverseTest)
try
{
    std::vector<String> candidate_strings = {"one week's time test", "abcdef", "abcabc", "moc.pacgnip"};
    std::vector<String> reversed_strings = {"tset emit s'keew eno", "fedcba", "cbacba", "pingcap.com"};

    // test vector
    ASSERT_COLUMN_EQ(toVec(reversed_strings), executeFunction("reverse", toVec(candidate_strings)));

    // test nullable
    ASSERT_COLUMN_EQ(
        toNullableVec({"", " ", {}, "pacgnip"}),
        executeFunction("reverse", toNullableVec({"", " ", {}, "pingcap"})));

    // test const
    ASSERT_COLUMN_EQ(toConst("pacgnip"), executeFunction("reverse", toConst("pingcap")));

    // test null
    ASSERT_COLUMN_EQ(toConst({}), executeFunction("reverse", toConst({})));
}
CATCH

// test reverseUTF8
TEST_F(StringReverse, stringReverseUTF8Test)
try
{
    std::vector<String> candidate_strings
        = {"one week's time test",
           "abc测试def",
           "abcテストabc",
           "ѐёђѓєѕіїјљњћќѝўџ",
           "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……љ&њ（ћ）ќ￥ѝ#ў@џ！^",
           "αβγδεζηθικλμνξοπρστυφχψωσ",
           "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★σ✕",
           "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"};
    std::vector<String> reversed_strings
        = {"tset emit s'keew eno",
           "fed试测cba",
           "cbaトステcba",
           "џўѝќћњљјїіѕєѓђёѐ",
           "^！џ@ў#ѝ￥ќ）ћ（њ&љ……ј%ї@$і#ѕ@є!ѓ/ђ*ё-ѐ+",
           "σωψχφυτσρποξνμλκιθηζεδγβα",
           "✕σ★ω♘ψχ♖φυ♥τσ℉ρπ✚οξ✓νμ♫λκ€ιθ✂ηζ☎εδ➨γβ▼α▲",
           "շմնբվցղզխլկյհգֆդսապօիւըտռեոքծժճչրջձփթ"};

    // test vector
    ASSERT_COLUMN_EQ(toVec(reversed_strings), executeFunction("reverseUTF8", toVec(candidate_strings)));

    // test nullable
    ASSERT_COLUMN_EQ(
        toNullableVec({"", " ", {}, "pacgnip"}),
        executeFunction("reverseUTF8", toNullableVec({"", " ", {}, "pingcap"})));

    // test const
    ASSERT_COLUMN_EQ(toConst("pacgnip"), executeFunction("reverseUTF8", toConst("pingcap")));

    // test null
    ASSERT_COLUMN_EQ(toConst({}), executeFunction("reverseUTF8", toConst({})));
}
CATCH

} // namespace DB::tests
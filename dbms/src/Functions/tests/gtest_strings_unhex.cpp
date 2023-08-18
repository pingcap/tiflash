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

#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB
{
namespace tests
{
class UnHexTest : public DB::tests::FunctionTest
{
};

TEST_F(UnHexTest, unhexAllUnitTest)
try
{
    const String & func_name = "tidbUnHex";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap.com", "abcd", std::nullopt, std::nullopt, ""}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"7777772E70696E676361702E636F6D", "61626364", std::nullopt, "GG", ""})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"ѐёђѓєѕіїјЉЊЋЌЍЎЏ",
             "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^",
             "αβγδεζηθικλμνξοπρστυφχψως",
             "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕",
             "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"}),
        executeFunction(
            func_name,
            createColumn<String>(
                {"d190d191d192d193d194d195d196d197d198d089d08ad08bd08cd08dd08ed08f",
                 "2bd1902dd1912ad1922fd19321d19440d19523d1962440d19725d198e280a6e280a6d08926d08aefbc88d08befbc89d08cefb"
                 "fa5d08d23d08e40d08fefbc815e",
                 "ceb1ceb2ceb3ceb4ceb5ceb6ceb7ceb8ceb9cebacebbcebccebdcebecebfcf80cf81cf83cf84cf85cf86cf87cf88cf89cf82",
                 "e296b2ceb1e296bcceb2ceb3e29ea8ceb4ceb5e2988eceb6ceb7e29c82ceb8ceb9e282accebacebbe299abcebccebde29c93c"
                 "ebecebfe29c9acf80cf81e28489cf83cf84e299a5cf85cf86e29996cf87cf88e29998cf89e29885cf82e29c95",
                 "d5a9d683d5b1d5bbd680d5b9d5b3d5aad5aed684d5b8d5a5d5bcd5bfd5a8d682d5abd685d5bad5a1d5bdd5a4d686d5a3d5b0d"
                 "5b5d5afd5acd5add5a6d5b2d681d5bed5a2d5b6d5b4d5b7"})));

    // CJK and emoji
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"さらに入", "测试测试测试测试abcd测试", "🍻", "🏴‍☠️"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"E38195E38289E381ABE585A5",
                 "E6B58BE8AF95E6B58BE8AF95E6B58BE8AF95E6B58BE8AF9561626364E6B58BE8AF95",
                 "F09F8DBB",
                 "F09F8FB4E2808DE298A0EFB88F"})));

    // Special Empty Character
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"\t", "\t", "\n", "\n", " "}),
        executeFunction(func_name, createColumn<Nullable<String>>({"9", "09", "A", "0A", "20"})));

    // Const Column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(4, "ab"),
        executeFunction(func_name, createConstColumn<String>(4, "6162")));
}
CATCH
} // namespace tests
} // namespace DB

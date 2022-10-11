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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class OctStringTest : public DB::tests::FunctionTest
{
protected:
    static constexpr auto func_name = "octString";

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};


TEST_F(OctStringTest, Simple)
try
{
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({"0", "1", "2", "10", "12"}),
        executeFunction(
            func_name,
            toNullableVec<String>({"0", "1", "2", "8", "10"})));

    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            toConst("8")));
}
CATCH

TEST_F(OctStringTest, Boundary)
try
{
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "0",
                               "0",
                               "0",
                               "0",
                               "10",
                               "20",
                               "0",
                               "1",
                               "10"}),
        executeFunction(
            func_name,
            toNullableVec<String>({{},
                                   "",
                                   "pingcap",
                                   "    pingcap",
                                   "ping cap",
                                   "8pingcap",
                                   "16 pingcap",
                                   "pingcap16",
                                   "1ping6 cap",
                                   "\n\t\r\f\v8pingcap"})));

    ASSERT_COLUMN_EQ(
        toNullableVec<String>({"0",
                               "0",
                               "0",
                               "0",
                               "1",
                               "10",
                               "1777777777777777777777",
                               "1777777777777777777777",
                               "1777777777777777777777",
                               "1777777777777777777777"}),
        executeFunction(
            func_name,
            toNullableVec<String>({"-+8",
                                   "+-8",
                                   "--8",
                                   "++8",
                                   "1.9",
                                   "+8",
                                   "-1",
                                   "-1.9",
                                   "99999999999999999999999999",
                                   "-99999999999999999999999999"})));
}
CATCH
} // namespace tests
} // namespace DB

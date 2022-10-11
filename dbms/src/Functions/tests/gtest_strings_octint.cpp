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

#include <limits>

namespace DB
{
namespace tests
{
class OctIntTest : public DB::tests::FunctionTest
{
protected:
    static constexpr auto func_name = "octInt";

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};

TEST_F(OctIntTest, Simple)
try
{
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({"0",
                               "1",
                               "2",
                               "4",
                               "10"}),
        executeFunction(
            func_name,
            toNullableVec<UInt64>({0,
                                   1,
                                   2,
                                   4,
                                   8})));
}
CATCH

TEST_F(OctIntTest, AllType)
try
{
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "0",
                               "377"}),
        executeFunction(
            func_name,
            toNullableVec<UInt8>({{},
                                  8,
                                  0,
                                  255})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<UInt8>(1, 8)));

    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "0",
                               "177777"}),
        executeFunction(
            func_name,
            toNullableVec<UInt16>({{},
                                   8,
                                   0,
                                   65535})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<UInt16>(1, 8)));
    
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "0",
                               "37777777777"}),
        executeFunction(
            func_name,
            toNullableVec<UInt32>({{},
                                   8,
                                   0,
                                   4294967295})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<UInt32>(1, 8)));
    
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "0",
                               "1777777777777777777777"}),
        executeFunction(
            func_name,
            toNullableVec<UInt64>({{},
                                   8,
                                   0,
                                   std::numeric_limits<UInt64>::max()})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<UInt64>(1, 8)));
    
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "1777777777777777777600",
                               "177"}),
        executeFunction(
            func_name,
            toNullableVec<Int8>({{},
                                 8,
                                 -128,
                                 127})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<Int8>(1, 8)));
    
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "1777777777777777700000",
                               "77777"}),
        executeFunction(
            func_name,
            toNullableVec<Int16>({{},
                                  8,
                                  std::numeric_limits<Int16>::min(),
                                  std::numeric_limits<Int16>::max()})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<Int16>(1, 8)));
    
    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "1777777777760000000000",
                               "17777777777"}),
        executeFunction(
            func_name,
            toNullableVec<Int32>({{},
                                  8,
                                  std::numeric_limits<Int32>::min(),
                                  std::numeric_limits<Int32>::max()})));

    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<Int32>(1, 8)));

    ASSERT_COLUMN_EQ(
        toNullableVec<String>({{},
                               "10",
                               "1000000000000000000000",
                               "777777777777777777777"}),
        executeFunction(
            func_name,
            toNullableVec<Int64>({{},
                                  8,
                                  std::numeric_limits<Int64>::min(),
                                  std::numeric_limits<Int64>::max()})));
    ASSERT_COLUMN_EQ(
        toConst("10"),
        executeFunction(
            func_name,
            createConstColumn<Int64>(1, 8)));
}
CATCH
} // namespace tests
} // namespace DB

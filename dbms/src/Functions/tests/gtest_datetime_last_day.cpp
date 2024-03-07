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

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestLastDay : public DB::tests::FunctionTest
{
};

TEST_F(TestLastDay, BasicTest)
try
{
    constexpr size_t n = 3;
    unsigned * buf = new unsigned[n];
    for (size_t i = 0; i < n + 1; ++i)
    {
        ASSERT_GE(buf[i], 0);
    }

    const String func_name = TiDBLastDayTransformerImpl<DataTypeMyDate::FieldType>::name;

    // Ignore invalid month error
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    dag_context.clearWarnings();

    // nullable column test
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>(
            {MyDate{2001, 2, 28}.toPackedUInt(),
             MyDate{2000, 2, 29}.toPackedUInt(),
             MyDate{2000, 6, 30}.toPackedUInt(),
             MyDate{2000, 5, 31}.toPackedUInt(),
             {}}),
        executeFunction(
            func_name,
            {createColumn<MyDate>(
                {MyDate{2001, 2, 10}.toPackedUInt(),
                 MyDate{2000, 2, 10}.toPackedUInt(),
                 MyDate{2000, 6, 10}.toPackedUInt(),
                 MyDate{2000, 5, 10}.toPackedUInt(),
                 MyDate{2000, 0, 10}.toPackedUInt()})}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<MyDate>>(
            {MyDate{2001, 2, 28}.toPackedUInt(),
             MyDate{2000, 2, 29}.toPackedUInt(),
             MyDate{2000, 6, 30}.toPackedUInt(),
             MyDate{2000, 5, 31}.toPackedUInt(),
             {}}),
        executeFunction(
            func_name,
            {createColumn<MyDateTime>(
                {MyDateTime{2001, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                 MyDateTime{2000, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
                 MyDateTime{2000, 6, 10, 10, 10, 10, 0}.toPackedUInt(),
                 MyDateTime{2000, 5, 10, 10, 10, 10, 0}.toPackedUInt(),
                 MyDateTime{2000, 0, 10, 10, 10, 10, 0}.toPackedUInt()})}));

    // const test
    UInt64 input[] = {
        MyDateTime{2001, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 2, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 6, 10, 10, 10, 10, 0}.toPackedUInt(),
        MyDateTime{2000, 5, 10, 10, 10, 10, 0}.toPackedUInt(),
    };

    UInt64 output[] = {
        MyDate{2001, 2, 28}.toPackedUInt(),
        MyDate{2000, 2, 29}.toPackedUInt(),
        MyDate{2000, 6, 30}.toPackedUInt(),
        MyDate{2000, 5, 31}.toPackedUInt(),
    };

    for (size_t i = 0; i < sizeof(input) / sizeof(UInt64); ++i)
    {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<MyDate>>(3, output[i]),
            executeFunction(func_name, {createConstColumn<MyDate>(3, input[i])}));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<MyDate>>(3, output[i]),
            executeFunction(func_name, {createConstColumn<Nullable<MyDate>>(3, input[i])}));
    }

    // const nullable test
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<MyDate>>(3, {}),
        executeFunction(func_name, {createConstColumn<Nullable<MyDate>>(3, {})}));

    // special const test, month is zero.
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<MyDate>>(3, {}),
        executeFunction(
            func_name,
            {createConstColumn<MyDate>(3, MyDateTime{2000, 0, 10, 10, 10, 10, 0}.toPackedUInt())}));

    dag_context.setFlags(ori_flags);
}
CATCH

} // namespace tests
} // namespace DB

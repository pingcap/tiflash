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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class StringASCII : public DB::tests::FunctionTest
{
};

TEST_F(StringASCII, strAndStrTest)
{
    {
        // test const
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(0, 0), executeFunction("ascii", createConstColumn<String>(0, "")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 38),
            executeFunction("ascii", createConstColumn<String>(1, "&ad")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(5, 38),
            executeFunction("ascii", createConstColumn<String>(5, "&ad")));
    }

    {
        // test vec
        ASSERT_COLUMN_EQ(createColumn<Int64>({}), executeFunction("ascii", createColumn<String>({})));
        ASSERT_COLUMN_EQ(
            createColumn<Int64>({230, 104, 72, 50, 35, 0}),
            executeFunction("ascii", createColumn<String>({"我a", "hello", "HELLO", "23333", "#%@#^", ""})));
    }

    {
        // test nullable const
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(0, 0),
            executeFunction("ascii", createConstColumn<Nullable<String>>(0, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(1, 97),
            executeFunction("ascii", createConstColumn<Nullable<String>>(1, "aaa")));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(3, 97),
            executeFunction("ascii", createConstColumn<Nullable<String>>(3, "aaa")));
    }

    {
        // test nullable vec
        std::vector<Int32> null_map{0, 1, 0, 1, 0, 0, 1};
        ASSERT_COLUMN_EQ(
            createNullableColumn<Int64>({0, 0, 97, 0, 233, 233, 0}, null_map),
            executeFunction(
                "ascii",
                createNullableColumn<String>({"", "a", "abcd", "嗯", "饼干", "馒头", "?？?"}, null_map)));
    }
}
} // namespace tests
} // namespace DB

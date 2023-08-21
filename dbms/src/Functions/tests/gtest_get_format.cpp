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
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB::tests
{
class GetFormatTest : public DB::tests::FunctionTest
{
public:
    static constexpr auto funcName = "getFormat";
};

TEST_F(GetFormatTest, testBoundary)
try
{
    // const(non-null), vector
    // time_type is a const with non null value
    // location is a vector containing null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"%m.%d.%Y", {}}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, "DATE"),
            createColumn<Nullable<String>>({"USA", {}})));

    // const(null), vector
    // time_type is a const with null value
    // location is a vector containing null
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(2, {}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, {}),
            createColumn<Nullable<String>>({"USA", {}})));

    // const(non-null), const(non-null)
    // time_type is a const with non null value
    // location is a const with non null value
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(2, "%m.%d.%Y"),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, "DATE"),
            createConstColumn<Nullable<String>>(2, "USA")));

    // const(non-null), const(null)
    // time_type is a const with non null value
    // location is a const with null value
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(2, {}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, "DATE"),
            createConstColumn<Nullable<String>>(2, {})));

    // The time_type is a system pre_defined macro, thus assume time_type column is const
    // Throw an exception is time_type is not ColumnConst
    ASSERT_THROW(
        executeFunction(
            funcName,
            createColumn<Nullable<String>>({"DATE", "TIME"}),
            createColumn<Nullable<String>>({"USA", {}})),
        DB::Exception);
}
CATCH

TEST_F(GetFormatTest, testMoreCases)
try
{
    // time_type: DATE
    // all locations
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"%m.%d.%Y", "%Y-%m-%d", "%Y-%m-%d", "%d.%m.%Y", "%Y%m%d"}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(5, "DATE"),
            createColumn<Nullable<String>>({"USA", "JIS", "ISO", "EUR", "INTERNAL"})));

    // time_type: DATETIME
    // all locations
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"%Y-%m-%d %H.%i.%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H.%i.%s", "%Y%m%d%H%i%s"}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(5, "DATETIME"),
            createColumn<Nullable<String>>({"USA", "JIS", "ISO", "EUR", "INTERNAL"})));

    // time_type: TIMESTAMP
    // all locations
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"%Y-%m-%d %H.%i.%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H.%i.%s", "%Y%m%d%H%i%s"}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(5, "TIMESTAMP"),
            createColumn<Nullable<String>>({"USA", "JIS", "ISO", "EUR", "INTERNAL"})));

    // time_type: TIME
    // all locations
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"%h:%i:%s %p", "%H:%i:%s", "%H:%i:%s", "%H.%i.%s", "%H%i%s"}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(5, "TIME"),
            createColumn<Nullable<String>>({"USA", "JIS", "ISO", "EUR", "INTERNAL"})));

    // the location is not in ("USA", "JIS", "ISO", "EUR", "INTERNAL")
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", ""}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, "TIME"),
            createColumn<Nullable<String>>({"CAN", ""})));

    // the time_type is not in ("DATE", "DATETIME", "TIMESTAMP", "TIME")
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", ""}),
        executeFunction(
            funcName,
            createConstColumn<Nullable<String>>(2, "TIMEINUTC"),
            createColumn<Nullable<String>>({"USA", "ISO"})));
}
CATCH

} // namespace DB::tests

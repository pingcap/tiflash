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
#include <TestUtils/ProjectionTestUtil.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB
{
namespace tests
{
class ExpandProjectionTest : public DB::tests::ProjectionTest
{
};

TEST_F(ExpandProjectionTest, literalProjection)
try
{
    // Project literal(null) as nullable<String>
    // scenario: expand project: "null" for a nullable varchar(x) column
    ColumnsWithTypeAndName columns;
    columns.push_back(createColumn<Nullable<String>>({"さらに入", "测试测试测试测试abcd测试", "🍻", "🏴‍☠️"}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(4, {}),
        executeProjection(std::vector<String>{"null"}, columns)[0]);

    // Project literal(null) as nullable<Int64>>
    // scenario: expand project: "null" for a nullable int column
    columns.clear();
    columns.push_back(createColumn<Nullable<Int64>>({1, 2, 3, 4}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(4, {}),
        executeProjection(std::vector<String>{"null"}, columns)[0]);

    // Project literal(null) as nullable<Float64>>
    // scenario: expand project: "null" for a nullable double column
    columns.clear();
    columns.push_back(createColumn<Nullable<Float64>>({1.0, 2.0, 3.0, 4.0}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(4, {}),
        executeProjection(std::vector<String>{"null"}, columns)[0]);

    // Project literal(null) as nullable<Float32>>
    // scenario: expand project: "null" for a nullable float column
    columns.clear();
    columns.push_back(createColumn<Nullable<Float32>>({1.0, 2.0, 3.0, 4.0}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float32>>(4, {}),
        executeProjection(std::vector<String>{"null"}, columns)[0]);

    // Project literal(null) as nullable<DataTypeMyDateTime>>
    // scenario: expand project: "null" for a nullable datetime column
    columns.clear();
    columns.push_back(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>({MyDateTime(2020, 2, 10, 0, 0, 0, 0).toPackedUInt()}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}),
        executeProjection(std::vector<String>{"null"}, columns)[0]);

    // Special case for grouping_id column with type as Uint64
    // Project literal(num) as <Uint64>
    // scenario: expand project: "1" for a grouping id column default as Uint64
    columns.clear();
    columns.push_back(createColumn<UInt64>({0, 0, 0, 0, 0}));
    ASSERT_COLUMN_EQ(createConstColumn<UInt64>(5, 1), executeProjection(std::vector<String>{"1"}, columns)[0]);
}
CATCH
} // namespace tests
} // namespace DB

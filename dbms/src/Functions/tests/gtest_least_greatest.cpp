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

#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

namespace DB::tests
{
class LeastGreatestTest : public DB::tests::FunctionTest
{
};

TEST_F(LeastGreatestTest, testLeast)
try
{
    const String & func_name = "tidbLeast";

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1, 3, 2, 1, 3, 2, 8}),
        executeFunction(
            func_name,
            createColumn<Int8>({2, 3, 4, 5, 6, 7, 8}),
            createColumn<Int8>({1, 3, 5, 7, 9, 11, 10}),
            createColumn<Int8>({3, 5, 7, 6, 3, 2, 16}),
            createColumn<Int8>({4, 3, 2, 1, 7, 8, 9}),
            createColumn<Int32>({5, 6, 7, 8, 10, 9, 8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7, 2, 3, 3, 2}),
        executeFunction(
            func_name,
            createColumn<Int16>({10, 2, 3, 4, 5}),
            createColumn<Int32>({7, 6, 5, 3, 4}),
            createColumn<Int64>({8, 9, 6, 3, 2})));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7, 2, 3, 3, 2}),
        executeFunction(
            func_name,
            createColumn<UInt16>({10, 2, 3, 4, 5}),
            createColumn<UInt16>({7, 6, 5, 3, 4}),
            createColumn<UInt16>({8, 9, 6, 3, 2})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7, 0, 1, 2, 6}),
        executeFunction(
            func_name,
            createColumn<Int8>({10, 0, 1, 2, 6}),
            createColumn<Int8>({7, 6, 5, 4, 9}),
            createColumn<Int64>({8, 3, 4, 5, 6})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1, 3, 2, 5, 4}),
        executeFunction(
            func_name,
            createColumn<Int8>({2, 5, 6, 7, 9}),
            createColumn<Int32>({1, 4, 3, 7, 8}),
            createColumn<Int64>({3, 8, 4, 6, 7}),
            createColumn<Int16>({4, 3, 2, 9, 8}),
            createColumn<Int8>({5, 7, 6, 5, 4})));

    // int unsigned + int = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({-1, -100, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int32>>({-1, -100, {}}),
            createColumn<UInt32>({100, 1, 1000})));

    // int unsigned + bigint = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({-1, -100, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({-1, -100, {}}),
            createColumn<UInt32>({100, 1, 1000})));

    // bigint unsigned + bigint unsigned = bigint unsigned
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({1, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({1, 1, {}}),
            createColumn<UInt64>({100, 100, 1000})));

    // bigint + bigint = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({-100, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({1, 1, {}}),
            createColumn<Int64>({-100, 100, 1000})));

    // consider null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>({}),
            createColumn<Nullable<Int16>>({4}),
            createColumn<Nullable<Int32>>({}),
            createColumn<Nullable<Int64>>({})));

    // real least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1.1, -1.4, -1.1, -1.3, 1.1, -3.3, -1.1, -3.48, -12.34, 0.0, 0.0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.4, -1.4, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.48, -12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));


    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({-2, 0, -12, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.55, 1.55, 0, 0.0, {}}),
            createColumn<Nullable<Int32>>({-2, 3, -12, 0, {}}),
            createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));


    // const-vector least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({-3, -11, -3, -3, -3, -5, -3}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(7, -2),
            createConstColumn<Nullable<Int64>>(7, -3),
            createColumn<Nullable<Int64>>({0, -11, 2, -3, 4, -5, 6})));

    // vector-const least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, 1, 2, 3}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    // const-const least
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, -3),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 5),
            createConstColumn<Nullable<Int64>>(1, -3)));

    // only null least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({{}, {}}),
            createColumn<Nullable<Int64>>({{}, {}})));

    // const and only null least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({{}, {}, {}, {}}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({18446744073709551614U}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({18446744073709551615U}),
            createColumn<Nullable<UInt64>>({18446744073709551614U})));

    // bigint unsgiend + bigint = Decimal, not support for now
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Int64>>({92233720368547720}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<UInt64>>({9223372036854775818U}),
    //         createColumn<Nullable<Int64>>({92233720368547720})));
}
CATCH

TEST_F(LeastGreatestTest, testGreatest)
try
{
    const String & func_name = "tidbGreatest";

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({5, 6, 7, 8, 10, 11, 16}),
        executeFunction(
            func_name,
            createColumn<Int8>({2, 3, 4, 5, 6, 7, 8}),
            createColumn<Int8>({1, 3, 5, 7, 9, 11, 10}),
            createColumn<Int8>({3, 5, 7, 6, 3, 2, 16}),
            createColumn<Int8>({4, 3, 2, 1, 7, 8, 9}),
            createColumn<Int32>({5, 6, 7, 8, 10, 9, 8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({10, 9, 6, 4, 5}),
        executeFunction(
            func_name,
            createColumn<Int64>({10, 2, 3, 4, 5}),
            createColumn<Int64>({7, 6, 5, 3, 4}),
            createColumn<Int64>({8, 9, 6, 3, 2})));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({10, 9, 6, 4, 5}),
        executeFunction(
            func_name,
            createColumn<UInt16>({10, 2, 3, 4, 5}),
            createColumn<UInt16>({7, 6, 5, 3, 4}),
            createColumn<UInt16>({8, 9, 6, 3, 2})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({10, 6, 5, 5, 9}),
        executeFunction(
            func_name,
            createColumn<Int8>({10, 0, 1, 2, 6}),
            createColumn<Int8>({7, 6, 5, 4, 9}),
            createColumn<Int64>({8, 3, 4, 5, 6})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({5, 8, 6, 9, 9}),
        executeFunction(
            func_name,
            createColumn<Int8>({2, 5, 6, 7, 9}),
            createColumn<Int32>({1, 4, 3, 7, 8}),
            createColumn<Int64>({3, 8, 4, 6, 7}),
            createColumn<Int16>({4, 3, 2, 9, 8}),
            createColumn<Int8>({5, 7, 6, 5, 4})));

    // int unsigned + int = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({100, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int32>>({-1, -100, {}}),
            createColumn<UInt32>({100, 1, 1000})));

    // int unsigned + bigint = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({100, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({-1, -100, {}}),
            createColumn<UInt32>({100, 1, 1000})));

    // bigint unsigned + bigint unsigned = bigint unsigned
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({100, 100, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({1, 1, {}}),
            createColumn<UInt64>({100, 100, 1000})));

    // bigint + bigint = bigint
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({1, 100, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({1, 1, {}}),
            createColumn<Int64>({-100, 100, 1000})));

    // consider null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>({}),
            createColumn<Nullable<Int16>>({4}),
            createColumn<Nullable<Int32>>({}),
            createColumn<Nullable<Int64>>({})));

    // real Greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1.4, 1.1, 1.3, -1.1, 3.3, 1.1, 3.3, -1.1, 12.34, 12.34, 0.0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.4, -1.4, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.48, -12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));


    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1.55, 3, 0, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.55, 1.55, 0, 0.0, {}}),
            createColumn<Nullable<Int32>>({-2, 3, -12, 0, {}}),
            createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));


    // const-vector greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, -2, 2, -2, 4, -2, 6}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(7, -2),
            createConstColumn<Nullable<Int64>>(7, -3),
            createColumn<Nullable<Int64>>({0, -11, 2, -3, 4, -5, 6})));

    // vector-const greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({3, 3, 3, 3}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    // const-const greatest
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, 5),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 5),
            createConstColumn<Nullable<Int64>>(1, -3)));

    // only null greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({{}, {}}),
            createColumn<Nullable<Int64>>({{}, {}})));

    // const and only null greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({{}, {}, {}, {}}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({18446744073709551615U}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({18446744073709551615U}),
            createColumn<Nullable<UInt64>>({18446744073709551614U})));

    // bigint unsgiend + bigint = Decimal, not support for now
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<UInt64>>({9223372036854775818U}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<UInt64>>({9223372036854775818U}),
    //         createColumn<Nullable<Int64>>({92233720368547720})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({923372036854775818}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({923372036854775818}),
            createColumn<Nullable<Int64>>({92233720368547720})));
}
CATCH

} // namespace DB::tests

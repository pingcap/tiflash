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

#include <TestUtils/ExecutorTestUtils.h>
namespace DB
{
namespace tests
{
class QueryExprTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"default", "test"},
            {{"a", TiDB::TP::TypeBit}, {"b", TiDB::TP::TypeBit}},
            {toNullableVec<UInt64>("a", {1, 2}), toNullableVec<UInt64>("b", {2, 6})});

        context.addMockTable(
            {"default", "test2"},
            {{"a", TiDB::TP::TypeLongLong}, {"b", TiDB::TP::TypeLongLong}, {"c", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("a", {1, 2, {}}),
             toNullableVec<Int64>("b", {2, 3, 3}),
             toNullableVec<Int64>("c", {3, 4, 5})});

        context.addMockTable(
            {"default", "test3"},
            {{"col_1", TiDB::TP::TypeLongLong}, {"col_2", TiDB::TP::TypeLongLong}, {"col_3", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("col_1", {666, 776, 777, {}, 999, {}}),
             toNullableVec<Int64>("col_2", {666, 777, 777, 777, {}, {}}),
             toNullableVec<Int64>("col_3", {666, 776, 777, 888, 999, 999})});

        context.addMockTable(
            {"default", "i8"},
            {{"a", TiDB::TP::TypeTiny}},
            {toNullableVec<Int8>("a", {0, 1, -1, 49, 50, -49, -50, 127, -128, {}})});

        context.addMockTable(
            {"default", "u8"},
            {{"a", TiDB::TP::TypeShort}},
            {toNullableVec<UInt8>("a", {0, 4, 5, 49, 50, 255, {}})});

        context.addMockTable(
            {"default", "i32"},
            {{"a", TiDB::TP::TypeLong}},
            {toNullableVec<Int64>(
                "a",
                {0, 4, 5, -4, -5, 499999999, 500000000, -499999999, -500000000, 2147483647, -2147483648, {}})});

        context.addMockTable(
            {"default", "u32"},
            {{"a", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("a", {0, 4, 5, 499999999, 500000000, 4294967295, {}})});

        context.addMockTable(
            {"default", "i64"},
            {{"a", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>(
                "a",
                {0,
                 4,
                 5,
                 -4,
                 -5,
                 499999999999999999,
                 500000000000000000,
                 -499999999999999999,
                 -500000000000000000,
                 {}})});

        context.addMockTable(
            {"default", "u64"},
            {{"a", TiDB::TP::TypeBit}},
            {toNullableVec<UInt64>("a", {0, 4, 5, 4999999999999999999, 5000000000000000000, {}})});


        // table t for round_with_frac tests
        std::vector<std::optional<TypeTraits<Int64>::FieldType>> inputs;
        for (Int64 i = -66; i <= 32; ++i)
            inputs.emplace_back(i);
        inputs.emplace_back(9223372036854775807);
        inputs.emplace_back(-9223372036854775807);
        inputs.emplace_back(std::nullopt);
        context.addMockTable({"default", "t"}, {{"i", TiDB::TP::TypeLongLong}}, {toNullableVec<Int64>("i", inputs)});


        context.addMockTable(
            {"default", "logical_op_test"},
            {{"id", TiDB::TP::TypeLongLong},
             {"c1", TiDB::TP::TypeLongLong},
             {"c2", TiDB::TP::TypeLongLong},
             {"c3", TiDB::TP::TypeLongLong},
             {"c4", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("id", {0, 1, 2, 3, 4, 5, 6, 7, 8}),
             toNullableVec<Int64>("c1", {1, 1, 1, 0, 0, 0, {}, {}, {}}),
             toNullableVec<Int64>("c2", {1, 0, {}, 1, 0, {}, 1, 0, {}}),
             toNullableVec<Int64>("c3", {1, 1, 1, 0, 0, 0, 0, 0, 0}),
             toNullableVec<Int64>("c4", {1, 0, 0, 1, 0, 0, 1, 0, 0})});
    }
};

TEST_F(QueryExprTestRunner, bitOp)
try
{
    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test where bitAnd(b,4) = 4"));
    }

    {
        auto cols = {toNullableVec<UInt64>({1, 2}), toNullableVec<UInt64>({2, 6})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test where bitOr(b,4) = 6"));
    }

    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test where bitXor(b,2) = 4"));
    }

    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select * from default.test where bitNot(b) = 18446744073709551609"));
    }

    {
        auto cols = {toNullableVec<UInt64>({1}), toNullableVec<UInt64>({2})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select * from default.test where bitNot(b) = 18446744073709551613"));
    }
}
CATCH

TEST_F(QueryExprTestRunner, ifOp)
try
{
    {
        auto cols = {toNullableVec<Int64>({1}), toNullableVec<Int64>({2}), toNullableVec<Int64>({3})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where if(a = 1, b, c) != c"));
    }

    {
        auto cols = {toNullableVec<Int64>({2, {}}), toNullableVec<Int64>({3, 3}), toNullableVec<Int64>({4, 5})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where if(a = 1, b, c) = c"));
    }

    {
        auto cols = {toNullableVec<Int64>({1, 2}), toNullableVec<Int64>({2, 3}), toNullableVec<Int64>({3, 4})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where if(a, b, c) != c"));
    }

    {
        auto cols = {toNullableVec<Int64>({{}}), toNullableVec<Int64>({3}), toNullableVec<Int64>({5})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where if(a, b, c) = c"));
    }
}
CATCH

TEST_F(QueryExprTestRunner, inOp)
try
{
    {
        auto cols = {toNullableVec<Int64>({777}), toNullableVec<Int64>({777}), toNullableVec<Int64>({777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 in (777,888)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({777, {}}), toNullableVec<Int64>({777, 777}), toNullableVec<Int64>({777, 888})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 in (777,888)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 777}), toNullableVec<Int64>({666, 777}), toNullableVec<Int64>({666, 777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 in (col_2,777,888)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 777, {}}),
               toNullableVec<Int64>({666, 777, 777}),
               toNullableVec<Int64>({666, 777, 888})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 in (col_2,777,888)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 776, 999}),
               toNullableVec<Int64>({666, 777, {}}),
               toNullableVec<Int64>({666, 776, 999})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 not in (777,888)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 776, 999, {}}),
               toNullableVec<Int64>({666, 777, {}, {}}),
               toNullableVec<Int64>({666, 776, 999, 999})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 not in (777,888)"));
    }

    {
        auto cols = {toNullableVec<Int64>({776}), toNullableVec<Int64>({777}), toNullableVec<Int64>({776})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 not in (col_2,777,888)"));
    }

    {
        auto cols = {toNullableVec<Int64>({776}), toNullableVec<Int64>({777}), toNullableVec<Int64>({776})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 not in (col_2,777,888)"));
    }

    {
        auto cols = {toNullableVec<Int64>({777}), toNullableVec<Int64>({777}), toNullableVec<Int64>({777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 in (777,888,null)"));
    }


    {
        auto cols
            = {toNullableVec<Int64>({777, {}}), toNullableVec<Int64>({777, 777}), toNullableVec<Int64>({777, 888})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 in (777,888,null)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 777}), toNullableVec<Int64>({666, 777}), toNullableVec<Int64>({666, 777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_1 in (col_2,777,888,null)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 777, {}}),
               toNullableVec<Int64>({666, 777, 777}),
               toNullableVec<Int64>({666, 777, 888})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where col_3 in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where col_1 not in (777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where col_3 not in (777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where col_1 not in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where col_3 not in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where null in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where null not in(col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where 777 not in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where 111 in (col_2,777,888,null)"));
    }

    {
        ASSERT_COLUMNS_EQ_UR(
            ColumnsWithTypeAndName{},
            executeRawQuery("select * from default.test3 where 111 not in (col_2,777,888,null)"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 776, 777, {}, 999, {}}),
               toNullableVec<Int64>({666, 777, 777, 777, {}, {}}),
               toNullableVec<Int64>({666, 776, 777, 888, 999, 999})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test3 where 777 in (col_2,777,888,null)"));
    }
}
CATCH

TEST_F(QueryExprTestRunner, roundWithFrac)
try
{
    // i8
    {
        auto cols
            = {toNullableVec<Int8>({0, 1, -1, 49, 50, -49, -50, 127, -128, {}}),
               toNullableVec<Int64>({0, 1, -1, 49, 50, -49, -50, 127, -128, {}}),
               toNullableVec<Int64>({0, 1, -1, 49, 50, -49, -50, 127, -128, {}}),
               toNullableVec<Int64>({0, 0, 0, 50, 50, -50, -50, 130, -130, {}}),
               toNullableVec<Int64>({0, 0, 0, 0, 100, 0, -100, 100, -100, {}}),
               toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 0, 0, 0, {}}),
               toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 0, 0, 0, {}})};

        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select a, round_with_frac_int(a, 1), round_with_frac_int(a, 0), round_with_frac_int(a, -1), "
                "round_with_frac_int(a, -2), round_with_frac_int(a, -3), round_with_frac_int(a, -4) from default.i8"));
    }

    // u8
    {
        auto cols
            = {toNullableVec<Int16>({0, 4, 5, 49, 50, 255, {}}),
               toNullableVec<UInt64>({0, 4, 5, 49, 50, 255, {}}),
               toNullableVec<UInt64>({0, 4, 5, 49, 50, 255, {}}),
               toNullableVec<UInt64>({0, 0, 10, 50, 50, 260, {}}),
               toNullableVec<UInt64>({0, 0, 0, 0, 100, 300, {}}),
               toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, {}}),
               toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, {}})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select a, round_with_frac_uint(a, 1), round_with_frac_uint(a, 0), round_with_frac_uint(a, "
                            "-1), round_with_frac_uint(a, -2), round_with_frac_uint(a, -3), round_with_frac_uint(a, "
                            "-4) from default.u8"));
    }

    // i32
    {
        auto cols
            = {toNullableVec<Int32>(
                   {0, 4, 5, -4, -5, 499999999, 500000000, -499999999, -500000000, 2147483647, -2147483648, {}}),
               toNullableVec<Int64>(
                   {0, 4, 5, -4, -5, 499999999, 500000000, -499999999, -500000000, 2147483647, -2147483648, {}}),
               toNullableVec<Int64>(
                   {0, 4, 5, -4, -5, 499999999, 500000000, -499999999, -500000000, 2147483647, -2147483648, {}}),
               toNullableVec<Int64>(
                   {0, 0, 10, 0, -10, 500000000, 500000000, -500000000, -500000000, 2147483650, -2147483650, {}}),
               toNullableVec<Int64>(
                   {0, 0, 0, 0, 0, 500000000, 500000000, -500000000, -500000000, 2147483600, -2147483600, {}}),
               toNullableVec<Int64>(
                   {0, 0, 0, 0, 0, 500000000, 500000000, -500000000, -500000000, 2100000000, -2100000000, {}}),
               toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 1000000000, 0, -1000000000, 2000000000, -2000000000, {}}),
               toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {}})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select a, round_with_frac_int(a, 1), round_with_frac_int(a, 0), round_with_frac_int(a, "
                            "-1), round_with_frac_int(a, -2), round_with_frac_int(a, -8), round_with_frac_int(a, -9), "
                            "round_with_frac_int(a, -10) from default.i32"));
    }

    // u32
    {
        auto cols = {
            toNullableVec<Int64>({0, 4, 5, 499999999, 500000000, 4294967295, {}}),
            toNullableVec<UInt64>({0, 4, 5, 499999999, 500000000, 4294967295, {}}),
            toNullableVec<UInt64>({0, 4, 5, 499999999, 500000000, 4294967295, {}}),
            toNullableVec<UInt64>({0, 0, 10, 500000000, 500000000, 4294967300, {}}),
            toNullableVec<UInt64>({0, 0, 0, 500000000, 500000000, 4294967300, {}}),
            toNullableVec<UInt64>({0, 0, 0, 500000000, 500000000, 4300000000, {}}),
            toNullableVec<UInt64>({0, 0, 0, 0, 1000000000, 4000000000, {}}),
            toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, {}}),


        };
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select a, round_with_frac_uint(a, 1), round_with_frac_uint(a, 0), round_with_frac_uint(a, "
                            "-1), round_with_frac_uint(a, -2), round_with_frac_uint(a, -8), round_with_frac_uint(a, "
                            "-9), round_with_frac_uint(a, -10) from default.u32"));
    }

    // i64

    {
        auto cols = {
            toNullableVec<Int64>(
                {0,
                 4,
                 5,
                 -4,
                 -5,
                 499999999999999999,
                 500000000000000000,
                 -499999999999999999,
                 -500000000000000000,
                 {}}),
            toNullableVec<Int64>(
                {0,
                 4,
                 5,
                 -4,
                 -5,
                 499999999999999999,
                 500000000000000000,
                 -499999999999999999,
                 -500000000000000000,
                 {}}),
            toNullableVec<Int64>(
                {0,
                 4,
                 5,
                 -4,
                 -5,
                 499999999999999999,
                 500000000000000000,
                 -499999999999999999,
                 -500000000000000000,
                 {}}),
            toNullableVec<Int64>(
                {0,
                 0,
                 10,
                 0,
                 -10,
                 500000000000000000,
                 500000000000000000,
                 -500000000000000000,
                 -500000000000000000,
                 {}}),
            toNullableVec<Int64>(
                {0, 0, 0, 0, 0, 500000000000000000, 500000000000000000, -500000000000000000, -500000000000000000, {}}),
            toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 1000000000000000000, 0, -1000000000000000000, {}}),
            toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 0, 0, 0, {}}),
            toNullableVec<Int64>({0, 0, 0, 0, 0, 0, 0, 0, 0, {}})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select a, round_with_frac_int(a, 1), round_with_frac_int(a, 0), round_with_frac_int(a, "
                            "-1), round_with_frac_int(a, -2), round_with_frac_int(a, -18), round_with_frac_int(a, "
                            "-19), round_with_frac_int(a, -20) from default.i64"));
    }

    // u64
    {
        auto cols
            = {toNullableVec<UInt64>({0, 4, 5, 4999999999999999999, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 4, 5, 4999999999999999999, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 4, 5, 4999999999999999999, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 0, 10, 5000000000000000000, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 0, 0, 5000000000000000000, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 0, 0, 5000000000000000000, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 0, 0, 5000000000000000000, 5000000000000000000, {}}),
               toNullableVec<UInt64>({0, 0, 0, 0, 10000000000000000000ul, {}}),
               toNullableVec<UInt64>({0, 0, 0, 0, 0, {}})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select a, round_with_frac_uint(a, 1), round_with_frac_uint(a, 0), round_with_frac_uint(a, "
                            "-1), round_with_frac_uint(a, -2), round_with_frac_uint(a, -16), round_with_frac_uint(a, "
                            "-18), round_with_frac_uint(a, -19), round_with_frac_uint(a, -20) from default.u64"));
    }

    // table t
    {
        std::vector<std::optional<TypeTraits<Int64>::FieldType>> col_0;
        for (Int64 i = -66; i <= 32; ++i)
            col_0.emplace_back(i);
        col_0.emplace_back(9223372036854775807);
        col_0.emplace_back(-9223372036854775807);
        col_0.emplace_back(std::nullopt);

        auto cols
            = {toNullableVec<Int64>(col_0),
               toNullableVec<Int64>(
                   {0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -100000000000000000,
                    -120000000000000000,
                    -123000000000000000,
                    -123500000000000000,
                    -123460000000000000,
                    -123457000000000000,
                    -123456800000000000,
                    -123456790000000000,
                    -123456789000000000,
                    -123456789100000000,
                    -123456789120000000,
                    -123456789123000000,
                    -123456789123500000,
                    -123456789123460000,
                    -123456789123457000,
                    -123456789123456800,
                    -123456789123456790,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    -123456789123456789,
                    0,
                    {}})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select i, round_with_frac_int(-123456789123456789, i) from default.t"));
    }

    // # TODO: these tests are disabled because cross join is not available.
    // #=> DBGInvoke dag('select sum(round_with_frac_int(a, i)) from default.i32 cross join default.t where a #>= 0 and i is not null')
    // #+------------------+
    // #| sum(round(a, i)) |
    // #+------------------+
    // #|     132001391875 |
    // #+------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_int(a, i)) from default.i32 cross join default.t where a < #0 and i is not null')
    // #+------------------+
    // #| sum(round(a, i)) |
    // #+------------------+
    // #|    -132001391908 |
    // #+------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_uint(a, i)) from default.u32 cross join default.t where a #is null and i is not null')
    // #+------------------+
    // #| sum(round(a, i)) |
    // #+------------------+
    // #|             NULL |
    // #+------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_uint(a, i)) from default.u32 cross join default.t where i #is not null')
    // #+------------------+
    // #| sum(round(a, i)) |
    // #+------------------+
    // #|     222093792609 |
    // #+------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_int(a, i)) from default.i64 cross join default.t where a #>= 0 and i is not null')
    // #+----------------------+
    // #| sum(round(a, i))     |
    // #+----------------------+
    // #| 51000000000000000274 |
    // #+----------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_int(a, i)) from default.i64 cross join default.t where a < #0 and i is not null')
    // #+-----------------------+
    // #| sum(round(a, i))      |
    // #+-----------------------+
    // #| -51000000000000000274 |
    // #+-----------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_uint(a, i)) from default.u64 cross join default.t where a #is null and i is not null')
    // #+------------------+
    // #| sum(round(a, i)) |
    // #+------------------+
    // #|             NULL |
    // #+------------------+
    // #
    // #=> DBGInvoke dag('select sum(round_with_frac_uint(a, i)) from default.u64 cross join default.t where i #is not null')
    // #+-----------------------+
    // #| sum(round(a, i))      |
    // #+-----------------------+
    // #| 520000000000000000274 |
    // #+-----------------------+
}
CATCH

TEST_F(QueryExprTestRunner, logicalOp)
try
{
    {
        auto cols
            = {toNullableVec<UInt64>({1, 0, {}, 0, 0, 0, {}, 0, {}}),
               toNullableVec<UInt64>({1, 0, 0, 0, 0, 0, 0, 0, 0}),
               toNullableVec<UInt64>({1, 1, 1, 0, 0, 0, 0, 0, 0}),
               toNullableVec<UInt64>({1, 1, 1, 1, 0, {}, 1, {}, {}}),
               toNullableVec<UInt64>({1, 1, 1, 1, 0, 0, 1, 0, 0}),
               toNullableVec<UInt64>({1, 1, 1, 0, 0, 0, {}, {}, {}}),
               toNullableVec<UInt64>({0, 1, {}, 1, 0, {}, {}, {}, {}}),
               toNullableVec<UInt64>({0, 1, 1, 1, 0, 0, 1, 0, 0}),
               toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, {}, {}, {}}),
               toNullableVec<UInt64>({0, 0, 0, 1, 1, 1, {}, {}, {}}),
               toNullableVec<UInt64>({0, 0, 0, 1, 1, 1, 1, 1, 1})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select and(c1,c2), and(c3,c4), and(c1,c3), or(c1,c2), or(c3,c4), or(c1,c3), xor(c1,c2), "
                            "xor(c3,c4), xor(c1,c3), not(c1), not(c3) from default.logical_op_test order by id"));
    }
}
CATCH
} // namespace tests
} // namespace DB

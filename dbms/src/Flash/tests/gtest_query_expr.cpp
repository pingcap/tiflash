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
        context.addMockTable({"default", "test"},
                             {{"a", TiDB::TP::TypeBit}, {"b", TiDB::TP::TypeBit}},
                             {toNullableVec<UInt64>("a", {1, 2}),
                              toNullableVec<UInt64>("b", {2, 6})});

        context.addMockTable({"default", "test2"},
                             {{"a", TiDB::TP::TypeLongLong}, {"b", TiDB::TP::TypeLongLong}, {"c", TiDB::TP::TypeLongLong}},
                             {toNullableVec<Int64>("a", {1, 2, {}}),
                              toNullableVec<Int64>("b", {2, 3, 3}),
                              toNullableVec<Int64>("c", {3, 4, 5})});
    }
};

TEST_F(QueryExprTestRunner, bitOp)
try
{
    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test where bitAnd(b,4) = 4"));
    }

    {
        auto cols = {toNullableVec<UInt64>({1, 2}), toNullableVec<UInt64>({2, 6})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test where bitOr(b,4) = 6"));
    }

    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test where bitXor(b,2) = 4"));
    }

    {
        auto cols = {toNullableVec<UInt64>({2}), toNullableVec<UInt64>({6})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test where bitNot(b) = 18446744073709551609"));
    }

    {
        auto cols = {toNullableVec<UInt64>({1}), toNullableVec<UInt64>({2})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test where bitNot(b) = 18446744073709551613"));
    }
}
CATCH

TEST_F(QueryExprTestRunner, ifOp)
try
{
    {
        auto cols = {toNullableVec<Int64>({1}), toNullableVec<Int64>({2}), toNullableVec<Int64>({3})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test2 where if(a = 1, b, c) != c"));
    }

    {
        auto cols = {toNullableVec<Int64>({2, {}}), toNullableVec<Int64>({3, 3}), toNullableVec<Int64>({4, 5})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test2 where if(a = 1, b, c) = c"));
    }

    {
        auto cols = {toNullableVec<Int64>({1, 2}), toNullableVec<Int64>({2, 3}), toNullableVec<Int64>({3, 4})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test2 where if(a, b, c) != c"));
    }

    {
        auto cols = {toNullableVec<Int64>({{}}), toNullableVec<Int64>({3}), toNullableVec<Int64>({5})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery(
                "select * from default.test2 where if(a, b, c) = c"));
    }
}
CATCH
} // namespace tests
} // namespace DB

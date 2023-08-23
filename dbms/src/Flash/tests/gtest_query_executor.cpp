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
class QueryExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"default", "test"},
            {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("col_1", {"test1", "test2", "test3"}),
             toNullableVec<Int64>("col_2", {666, 666, 777})});

        context.addMockTable(
            {"default", "test2"},
            {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("col_1", {"test1", "test2"}), toNullableVec<Int64>("col_2", {666, 777})});

        context.addMockTable(
            {"default", "test3"},
            {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("col_1", {"test1", "test1"}), toNullableVec<Int64>("col_2", {666, 666})});

        auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
        auto date_col_ptr = createColumn<DataTypeMyDate::FieldType>(
                                {MyDate(2021, 1, 1).toPackedUInt(), MyDate(2020, 12, 31).toPackedUInt()})
                                .column;
        auto date_col = ColumnWithTypeAndName(date_col_ptr, date_type_ptr, "col_2");
        context.addMockTable(
            {"default", "test4"},
            {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeDate}},
            {toNullableVec<String>("col_1", {"test1", "test2"}), date_col});

        context.addMockTable(
            {"default", "test5"},
            {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("col_1", {"test1", "test2", {}}), toNullableVec<Int64>("col_2", {666, 777, {}})});
    }
};


TEST_F(QueryExecutorTestRunner, aggregation)
try
{
    {
        auto cols = {toVec<UInt64>({2, 1}), toNullableVec<Int64>({666, 777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select count(col_1), col_2 from default.test group by col_2"));
    }

    {
        auto cols = {toVec<UInt64>({2, 1}), toVec<UInt64>({2, 1}), toNullableVec<Int64>({666, 777})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select count(col_1),count(col_1),col_2 from default.test group by col_2"));
    }

    {
        auto cols = {toVec<UInt64>({2}), toNullableVec<Int64>({666})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select count(col_1),col_2 from default.test where col_2 = 666 group by col_2"));
    }
}
CATCH

TEST_F(QueryExecutorTestRunner, filter)
try
{
    {
        auto cols = {toNullableVec<String>({"test1"}), toNullableVec<Int64>({666})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where col_2 = 666"));
    }

    {
        auto cols = {toNullableVec<Int64>({777})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select col_2 from default.test2 where col_1 = 'test2'"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({666, 777}),
               toNullableVec<String>({"test1", "test2"}),
               toNullableVec<Int64>({666, 777})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select col_2, col_1, col_2 from default.test2 where col_1 = 'test2' or col_2 = 666"));
    }

    {
        auto cols = {toNullableVec<String>({"test1"}), toNullableVec<Int64>({666})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test2 where col_2 < 777 or col_2 > 888"));
    }

    {
        auto cols = {toNullableVec<Int64>({777}), toNullableVec<String>({"test2"}), toNullableVec<Int64>({777})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select col_2, col_1, col_2 from default.test2 where col_1 = 'test2' and col_2 = 777"));
    }
}
CATCH

TEST_F(QueryExecutorTestRunner, limit)
try
{
    {
        auto cols = {toNullableVec<Int64>({666}), toNullableVec<String>({"test1"}), toNullableVec<Int64>({666})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select col_2, col_1, col_2 from default.test3 where col_2 = 666 limit 1"));
    }
}
CATCH

TEST_F(QueryExecutorTestRunner, project)
try
{
    {
        auto cols = {toNullableVec<String>({"2021-01", "2020-12"})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select date_format(col_2, \'%Y-%m\') from default.test4"));
    }
}
CATCH

TEST_F(QueryExecutorTestRunner, topn)
try
{
    {
        auto cols = {toNullableVec<String>({{}, "test1"}), toNullableVec<Int64>({{}, 666})};
        ASSERT_COLUMNS_EQ_UR(cols, executeRawQuery("select * from default.test5 order by col_2 limit 2"));
    }

    {
        auto cols
            = {toNullableVec<Int64>({777, 666}),
               toNullableVec<String>({"test2", "test1"}),
               toNullableVec<Int64>({777, 666})};
        ASSERT_COLUMNS_EQ_UR(
            cols,
            executeRawQuery("select col_2, col_1, col_2 from default.test5 where col_1 = 'test2' or col_2 = 666 order "
                            "by col_1 desc limit 2"));
    }
}
CATCH
} // namespace tests
} // namespace DB

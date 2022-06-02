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

#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include "Parsers/ASTTablesInSelectQuery.h"

namespace DB
{
namespace tests
{
class ExecutorTest : public DB::tests::InterpreterTest
{
public:
    void initializeContext() override
    {
        InterpreterTest::initializeContext();
        context.addMockTableWithColumnData({"test_db", "test_table"},
                                           {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                           {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                            toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiverWithColumnData("exchange1",
                                                  {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                                  {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                                   toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTableWithColumnData({"test_db", "r_table"},
                                           {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                           {toVec<String>("s", {"banana", "banana"}),
                                            toVec<String>("join_c", {"apple", "banana"})});

        context.addMockTableWithColumnData({"test_db", "l_table"},
                                           {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                           {toVec<String>("s", {"banana", "banana"}),
                                            toVec<String>("join_c", {"apple", "banana"})});
    }

    template <typename T>
    ColumnWithTypeAndName toNullableVec(String name, const std::vector<std::optional<typename TypeTraits<T>::FieldType>> & v)
    {
        return createColumn<Nullable<T>>(v, name);
    }

    template <typename T>
    ColumnWithTypeAndName toVec(String name, const std::vector<typename TypeTraits<T>::FieldType> & v)
    {
        return createColumn<T>(v, name);
    }
};

TEST_F(ExecutorTest, Filter)
try
{
    auto request = context
                       .scan("test_db", "test_table")
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);
    {
        executeStreams(request,
                       {toNullableVec<String>("s1", {"banana"}),
                        toNullableVec<String>("s2", {"banana"})});
    }

    request = context.scan("test_db", "test_table")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    {
        executeStreams(request,
                       {toNullableVec<String>("s1", {"banana"}),
                        toNullableVec<String>("s2", {"banana"})});
    }

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    {
        executeStreams(request,
                       {toNullableVec<String>("s1", {"banana"}),
                        toNullableVec<String>("s2", {"banana"})});
    }

    request = context.scan("test_db", "l_table").join(context.scan("test_db", "r_table"), {col("join_c")}, ASTTableJoin::Kind::Left).build(context);
    {
        String expected = 
        "Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
        " table_scan_0 | {<0, String>, <1, String>}\n"
        " table_scan_1 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>("s", {"banana", "banana"}),
                        toNullableVec<String>("join_c", {"apple", "banana"}),
                        toNullableVec<String>("s", {"banana", "banana"}),
                        toNullableVec<String>("join_c", {"apple", "banana"})});
    }
}
CATCH

} // namespace tests
} // namespace DB
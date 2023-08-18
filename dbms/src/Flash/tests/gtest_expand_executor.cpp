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
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExpandExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange1",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});
    }
};

TEST_F(ExpandExecutorTestRunner, ExpandLogical)
try
{
    /// case 1
    auto request = context
                       .scan("test_db", "test_table")
                       .expand(MockVVecColumnNameVec{
                           MockVecColumnNameVec{
                               MockColumnNameVec{"s1"},
                           },
                           MockVecColumnNameVec{
                               MockColumnNameVec{"s2"},
                           },
                       })
                       .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2      groupingID
    ///  "banana"  NULL         1
    ///   NULL    "apple"       2
    ///   NULL     NULL         1
    ///   NULL     NULL         2
    ///  "banana"  NULL         1
    ///   NULL   "banana"       2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", {}, {}, {}, "banana", {}}),
         toNullableVec<String>({{}, "apple", {}, {}, {}, "banana"}),
         toVec<UInt64>({1, 2, 1, 2, 1, 2})});

    /// case 2
    request = context
                  .scan("test_db", "test_table")
                  .filter(eq(col("s1"), col("s2")))
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s1"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2      groupingID
    ///  "banana"  NULL         1
    ///   NULL   "banana"       2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", {}}),
         toNullableVec<String>({{}, "banana"}),
         toVec<UInt64>({1, 2})});

    /// case 3: this case is only for non-planner mode.
    /// request = context
    ///                 .scan("test_db", "test_table")
    ///                 .expand(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"s1"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
    ///                 .filter(eq(col("s1"), col("s2")))
    ///                 .build(context);
    /// data flow: TiFlash isn't aware of the operation sequence, this filter here will be run before expand does just like the second test case above.
    /// since this case is only succeed under planner-disabled mode, just comment and assert the result here for a note.
    ///
    /// executeAndAssertColumnsEqual(
    ///        request,
    ///        {toNullableVec<String>({"banana", {}}),
    ///        toNullableVec<String>({{}, "banana"}),
    ///        toVec<UInt64>({1,2})});

    /// case 4
    auto const_false = lit(Field(static_cast<UInt64>(0)));
    request = context
                  .scan("test_db", "test_table")
                  .filter(const_false) // refuse all rows
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s1"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {});

    /// case 5   (test integrated with aggregation)
    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {
            toVec<UInt64>({1, 0, 1}),
            toNullableVec<String>({"apple", {}, "banana"}),
        });

    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"count(s1)"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///  count(s1)   s2
    ///    1      "apple"
    ///    0       NULL
    ///    1      "banana"
    ///          |
    ///          v
    ///  count(s1)   s2      groupingID
    ///    1        NULL        1
    ///   NULL     "apple"      2
    ///    0        NULL        1
    ///   NULL      NULL        2
    ///    1        NULL        1
    ///   NULL     "banana"     2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({1, {}, 0, {}, 1, {}}),
         toNullableVec<String>({{}, "apple", {}, {}, {}, "banana"}),
         toVec<UInt64>({1, 2, 1, 2, 1, 2})});

    /// case 5   (test integrated with aggregation and projection)
    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"count(s1)"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .project({"count(s1)"})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({1, {}, 0, {}, 1, {}})});

    /// case 6   (test integrated with aggregation and projection and limit) 1
    /// note: by now, limit is executed before expand does to reduce unnecessary row expand work.
    /// request = context
    ///               .scan("test_db", "test_table")
    ///               .aggregation({Count(col("s1"))}, {col("s2")})
    ///               .expand(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"count(s1)"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
    ///               .limit(2)
    ///               .project({"count(s1)"})
    ///               .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///  count(s1)   s2
    ///    1      "apple"
    ///    0       NULL
    ///    1      "banana"
    ///          |
    ///          v
    ///  count(s1)   s2                    // limit precede the expand OP since they are in the same DAG query block.
    ///    1      "apple"
    ///    0       NULL
    ///          |
    ///          v
    ///  count(s1)   s2      groupingID    // expand is always arranged executed after limit to avoid unnecessary replication in the same DAG query block.
    ///    1        NULL        1
    ///   NULL     "apple"      2
    ///    0        NULL        1
    ///   NULL      NULL        2
    ///    1        NULL        1
    ///   NULL     "banana"     2
    ///          |
    ///          v
    ///  count(s1)
    ///    1
    ///   NULL
    ///    0
    ///   NULL
    ///
    /// since this case is only succeed under planner-disabled mode, just comment and assert the result here for a note.
    ///
    /// executeAndAssertColumnsEqual(
    ///   request,
    ///   {toNullableVec<UInt64>({1, {}, 0, {}})});

    /// case 7   (test integrated with aggregation and projection and limit) 2
    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"count(s1)"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .project({"count(s1)"})
                  .topN({{"count(s1)", true}}, 2)
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2                                         ---------------+
    /// "banana"  "apple"                                                     |
    ///   NULL      NULL                  // table scan                       |
    /// "banana"  "banana"                                                    |
    ///          |                                                            |
    ///          v                                                            |
    ///  count(s1)   s2                                                       |
    ///    1      "apple"                 // aggregate                        |
    ///    0       NULL                                                       |
    ///    1      "banana"                                                    |
    ///          |                                                            +------------->  Child DAG Query Block
    ///          v                                                            |
    ///  count(s1)   s2      groupingID   // expand                           |
    ///    1        NULL        1                                             |
    ///   NULL     "apple"      2                                             |
    ///    0        NULL        1                                             |
    ///   NULL      NULL        2                                             |
    ///    1        NULL        1                                             |
    ///   NULL     "banana"     2                                             |
    ///          |                                              --------------+
    ///          v                                              --------------+
    ///  count(s1)                                                            |
    ///    1                                                                  |
    ///   NULL                             // projection                      |
    ///    0                                                                  |
    ///   NULL                                                                |
    ///    1                                                                  +------------->  parent DAG Query Block
    ///   NULL                                                                |
    ///          |                                                            |
    ///          v                                                            |
    ///  count(s1)                         // sort (desc)                     |
    ///    1                                                                  |
    ///    1                                                                  |
    ///    0                                                                  |
    ///   NULL                                                                |
    ///   NULL                                                                |
    ///   NULL                                                                |
    ///          |                                                            |
    ///          v                                                            |
    ///   count(s1)                        // limit 2                         |
    ///    1                                                                  |
    ///    1                                                                  |
    ///                                                        ---------------+
    ///
    ///  Note: you can see some difference from this plan and the last one above, since projection between expand and topN is a SOURCE node,
    ///        it will isolate whole DAG into two independent DAG query blocks, limit and expand OP take a place in each one of them. So we
    ///        couldn't guarantee that letting expand OP run after limit does, which can't reduce unnecessary replication work. DAG query block
    ///        division should be blamed here.
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({1, 1})});

    /// case 8  (test integrated with receiver and join)
    request = context
                  .receive("exchange1")
                  .join(context.scan("test_db", "test_table").project({"s2"}), tipb::JoinType::TypeInnerJoin, {col("s2")})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", "banana"}),
         toNullableVec<String>({"apple", "banana"}),
         toNullableVec<String>({"apple", "banana"})});

    request = context
                  .receive("exchange1")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"count(s1)"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .join(context.scan("test_db", "test_table").project({"s2"}), tipb::JoinType::TypeInnerJoin, {col("s2")})
                  .project({"count(s1)", "groupingID"})
                  .topN({{"groupingID", true}}, 2)
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2                                         ---------------+
    /// "banana"  "apple"                                                     |
    ///   NULL      NULL                  // table scan                       |
    /// "banana"  "banana"                                                    |
    ///          |                                                            |
    ///          v                                                            |
    ///  count(s1)   s2                                                       |
    ///    1      "apple"                 // aggregate                        |
    ///    0       NULL                                                       |
    ///    1      "banana"                                                    |
    ///          |                                                            +------------->  Child of Child DAG Query Block
    ///          v                                                            |
    ///  count(s1)   s2      groupingID   // expand                           |
    ///    1        NULL        1                                             |
    ///   NULL     "apple"      2                                             |
    ///    0        NULL        1                                             |
    ///   NULL      NULL        2                                             |
    ///    1        NULL        1                                             |
    ///   NULL     "banana"     2                                             |
    ///          |                                              --------------+
    ///          v                                              --------------+
    ///  count(s1)   s2      groupingID  *    s2                              |
    ///   NULL     "apple"      2           "apple"       // join             |
    ///   NULL     "banana"     2            NULL                             |
    ///                                     "banana"                          +------------->  Child DAG Query Block
    ///                                                                       |
    ///   NULL     "apple"      2          "apple"                            |
    ///   NULL     "banana"     2          "banana"                           |
    ///          |                                             ---------------+
    ///          v                                                            |
    ///  count(s1)  groupingID             // projection                      |
    ///   NULL         2                                                      |
    ///   NULL         2                                                      |
    ///          |                                                            +------------->  Parent DAG Query Block
    ///          v                                                            |
    ///   count(s1)  groupingID            // topN                            |
    ///   NULL         2                                                      |
    ///   NULL         2                                                      |
    ///                                                        ---------------+
    ///
    executeAndAssertColumnsEqual(
        request,
        {
            toNullableVec<UInt64>({{}, {}}),
            toVec<UInt64>({2, 2}),
        });
}
CATCH

/// TODO: more OP combination tests.

} // namespace tests
} // namespace DB

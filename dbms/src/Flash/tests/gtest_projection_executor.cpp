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
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class ExecutorProjectionTestRunner : public DB::tests::ExecutorTest
{
public:
    using ColDataString = std::vector<std::optional<typename TypeTraits<String>::FieldType>>;
    using ColDataInt32 = std::vector<std::optional<typename TypeTraits<Int32>::FieldType>>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                             {{col_names[0], TiDB::TP::TypeString},
                              {col_names[1], TiDB::TP::TypeString},
                              {col_names[2], TiDB::TP::TypeString},
                              {col_names[3], TiDB::TP::TypeLong},
                              {col_names[4], TiDB::TP::TypeLong}},
                             {toNullableVec<String>(col_names[0], col0),
                              toNullableVec<String>(col_names[1], col1),
                              toNullableVec<String>(col_names[2], col2),
                              toNullableVec<Int32>(col_names[3], col3),
                              toNullableVec<Int32>(col_names[4], col4)});
    }

    template <typename T>
    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(T param)
    {
        return context.scan(db_name, table_name).project(param).build(context);
    };

    void executeWithConcurrency(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
    {
        for (size_t i = 1; i < 10; i += 2)
        {
            ASSERT_COLUMNS_EQ_UR(executeStreams(request, i), expect_columns);
        }
    }

    /// Prepare column data
    const ColDataString col0{"col0-0", "col0-1", "", "col0-2", {}, "col0-3", ""};
    const ColDataString col1{"col1-0", {}, "", "col1-1", "", "col1-2", "col1-3"};
    const ColDataString col2{"", "col2-0", "col2-1", {}, "col2-3", {}, "col2-4"};
    const ColDataInt32 col3{1, {}, 0, -111111, {}, 0, 9999};

    /** Each value in col4 should be different from each other so that topn 
     *  could sort the columns into an unique result, or multi-results could
     *  be right.
     */
    const ColDataInt32 col4{0, 5, -123, -234, {}, 24353, 9999};

    /// Results after sorted by col4
    const ColDataString col0_sorted_asc{{}, "col0-2", "", "col0-0", "col0-1", "", "col0-3"};
    const ColDataString col1_sorted_asc{"", "col1-1", "", "col1-0", {}, "col1-3", "col1-2"};
    const ColDataString col2_sorted_asc{"col2-3", {}, "col2-1", "", "col2-0", "col2-4", {}};
    const ColDataInt32 col3_sorted_asc{{}, -111111, 0, 1, {}, 9999, 0};
    const ColDataInt32 col4_sorted_asc{{}, -234, -123, 0, 5, 9999, 24353};

    /// Prepare some names
    std::vector<String> col_names{"col0", "col1", "col2", "col3", "col4"};
    const String db_name{"test_db"};
    const String table_name{"projection_test_table"};
};

TEST_F(ExecutorProjectionTestRunner, Projection)
try
{
    /// Check single column
    auto request = buildDAGRequest<MockColumnNameVec>({col_names[4]});
    executeWithConcurrency(request, {toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Check multi columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[0], col_names[4]});
    executeWithConcurrency(request,
                           {
                               toNullableVec<String>(col_names[0], col0_sorted_asc),
                               toNullableVec<Int32>(col_names[4], col4_sorted_asc),
                           });

    /// Check multi columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[0], col_names[1], col_names[4]});
    executeWithConcurrency(request,
                           {toNullableVec<String>(col_names[0], col0_sorted_asc),
                            toNullableVec<String>(col_names[1], col1_sorted_asc),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Check duplicate columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[4], col_names[4], col_names[4]});
    executeWithConcurrency(request,
                           {toNullableVec<Int32>(col_names[4], col4_sorted_asc),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    {
        /// Check large number of columns
        const size_t col_num = 100;
        MockColumnNameVec projection_input;
        ColumnsWithTypeAndName columns;
        auto expect_column = toNullableVec<Int32>(col_names[4], col4_sorted_asc);

        for (size_t i = 0; i < col_num; ++i)
        {
            projection_input.push_back(col_names[4]);
            columns.push_back(expect_column);
        }

        request = buildDAGRequest<MockColumnNameVec>(projection_input);
        executeWithConcurrency(request, columns);
    }
}
CATCH

TEST_F(ExecutorProjectionTestRunner, ProjectionFunction)
try
{
    std::shared_ptr<tipb::DAGRequest> request;

    /// Test "equal" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({eq(col(col_names[0]), col(col_names[0])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 1, 1, 1, 1, 1, 1}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({eq(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 1, 0, {}, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({eq(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 0, 0, {}, 1, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});


    /// Test "greater" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({gt(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 0, 0, {}, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({gt(col(col_names[1]), col(col_names[0])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 1, 0, 1, {}, 1, 1}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({gt(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 1, 1, {}, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({gt(col(col_names[4]), col(col_names[3])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 1, 0, 0, {}, 0, 1}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});


    /// Test "and" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({And(col(col_names[0]), col(col_names[0])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 0, 0, 0, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({And(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({And(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 1, 0, 0, {}, 1, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Test "not" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({NOT(col(col_names[0])), NOT(col(col_names[1])), NOT(col(col_names[2])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 1, 1, 1, 1, 1, 1}),
                            toNullableVec<UInt64>({1, 1, 1, 1, {}, 1, 1}),
                            toNullableVec<UInt64>({1, {}, 1, 1, 1, 1, {}}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({NOT(col(col_names[3])), NOT(col(col_names[4])), col(col_names[4])});
    executeWithConcurrency(request,
                           {toNullableVec<UInt64>({{}, 0, 1, 0, {}, 0, 1}),
                            toNullableVec<UInt64>({{}, 0, 0, 1, 0, 0, 0}),
                            toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// TODO more functions...
}
CATCH

} // namespace tests
} // namespace DB

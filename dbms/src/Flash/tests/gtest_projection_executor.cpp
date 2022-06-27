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

    template<typename T>
    std::shared_ptr<tipb::DAGRequest> getRequest(T param)
    {
        return context.scan(db_name, table_name).project(param).build(context);
    };

    /// Prepare column data
    const ColDataString col0{"col0-0", "col0-1",       "", "col0-2",       {}, "col0-3",       ""};
    const ColDataString col1{"col1-0",       {},       "", "col1-1",       "", "col1-2", "col1-3"};
    const ColDataString col2{      "", "col2-0", "col2-1",       {}, "col2-3",       {}, "col2-4"};
    const ColDataInt32  col3{1, {},  0, -111111, {},     0,   9999};
    const ColDataInt32  col4{0,  5, {},    -234, {}, 24353,   9999};

    /// Prepare some names
    std::vector<String> col_names{"col0", "col1", "col2", "col3", "col4"};
    const String db_name{"test_db"};
    const String table_name{"projection_test_table"};
};

TEST_F(ExecutorProjectionTestRunner, Projection)
try
{
    /// Check single column
    auto request = getRequest<MockColumnNames>({col_names[2]});
    executeStreams(request, {toNullableVec<String>(col_names[2], col2)});

    /// Check multi columns
    request = getRequest<MockColumnNames>({col_names[0], col_names[4]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[4], col1),
                   });
    /// Check multi columns
    request = getRequest<MockColumnNames>({col_names[0], col_names[1], col_names[3]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[1], col1),
                    toNullableVec<String>(col_names[3], col2)});
    
    /// Check duplicate columns
    request = getRequest<MockColumnNames>({col_names[1], col_names[1], col_names[1]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[1], col1),
                    toNullableVec<String>(col_names[1], col1),
                    toNullableVec<String>(col_names[1], col1)});

    {
        /// Check large number of columns
        const size_t col_num = 100;
        MockColumnNamesVec projection_input;
        ColumnsWithTypeAndName columns;
        auto expect_column = toNullableVec<String>(col_names[1], col1);

        for (size_t i = 0; i < col_num; ++i)
        {
            projection_input.push_back(col_names[1]);
            columns.push_back(expect_column);
        }
        
        request = getRequest<MockColumnNamesVec>(projection_input);
        executeStreams(request, columns);
    }
}
CATCH

TEST_F(ExecutorProjectionTestRunner, ProjectionFunction)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    {
        /// Test "equal" function
        {
            /// Data type: TypeString
            request = getRequest<MockAsts>({eq(col(col_names[0]), col(col_names[0]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({1, 1, 1, 1, {}, 1, 1})});
            
            request = getRequest<MockAsts>({eq(col(col_names[0]), col(col_names[1]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, {}, 1, 0, {}, 0, 0})});
        }

        {
            /// Data type: TypeLong
            request = getRequest<MockAsts>({eq(col(col_names[3]), col(col_names[4]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, {}, {}, 0, {}, 0, 1})});
        }
    }

    {
        /// Test "greater" function
        {
            /// Data type: TypeString
            request = getRequest<MockAsts>({gt(col(col_names[0]), col(col_names[1]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, {}, 0, 0, {}, 0, 0})});

            request = getRequest<MockAsts>({gt(col(col_names[1]), col(col_names[0]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({1, {}, 0, 1, {}, 1, 1})});
        }

        {
            /// Data type: TypeLong
            request = getRequest<MockAsts>({gt(col(col_names[3]), col(col_names[4]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({1, {}, {}, 0, {}, 0, 0})});

            request = getRequest<MockAsts>({gt(col(col_names[4]), col(col_names[3]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, {}, {}, 1, {}, 1, 0})});
        }
    }

    {
        /// Test "and" function
        {
            /// Data type: TypeString
            request = getRequest<MockAsts>({And(col(col_names[0]), col(col_names[0]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, 0, 0, 0, {}, 0, 0})});

            request = getRequest<MockAsts>({And(col(col_names[0]), col(col_names[1]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, 0})});
        }

        {
            /// Data type: TypeLong
            request = getRequest<MockAsts>({And(col(col_names[3]), col(col_names[4]))});
            executeStreams(request,
                        {toNullableVec<UInt64>({0, {}, 0, 1, {}, 0, 1})});
        }
    }

    {
        /// Test "not" function
        {       
            /// Data type: TypeString
            request = getRequest<MockAsts>({NOT(col(col_names[0])), NOT(col(col_names[1])), NOT(col(col_names[2]))});
            executeStreams(request,
                           {toNullableVec<UInt64>({1, 1, 1, 1, {}, 1, 1}),
                            toNullableVec<UInt64>({1, {}, 1, 1, 1, 1, 1}),
                            toNullableVec<UInt64>({1, 1, 1, {}, 1, {}, 1})});
        }

        {
            /// Data type: TypeLong
            request = getRequest<MockAsts>({NOT(col(col_names[3])), NOT(col(col_names[4]))});
            executeStreams(request,
                           {toNullableVec<UInt64>({0, {}, 1, 0, {}, 1, 0}),
                            toNullableVec<UInt64>({1, 0, {}, 0, {}, 0, 0})});
        }
    }

    {
        /// Test "max" function
        /// TODO wait for the implementation of the max aggregation
        {
            /// Data type: TypeString
        }

        {
            /// Data type: TypeLong
        }
    }

    /// TODO more functions
}
CATCH

} // namespace tests
} // namespace DB

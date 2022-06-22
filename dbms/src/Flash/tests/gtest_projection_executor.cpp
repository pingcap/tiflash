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
    using ColDataType = std::vector<std::optional<typename TypeTraits<String>::FieldType>>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                            {{col_names[0], TiDB::TP::TypeString},
                            {col_names[1], TiDB::TP::TypeString},
                            {col_names[2], TiDB::TP::TypeString}},
                            {toNullableVec<String>(col_names[0], col0),
                            toNullableVec<String>(col_names[1], col1),
                            toNullableVec<String>(col_names[2], col2)});
    }

    std::shared_ptr<tipb::DAGRequest> getRequest(MockColumnNames col_names)
    {
        return context.scan(db_name, table_name).project(col_names).build(context);
    };

    /// Prepare column data
    const ColDataType col0{"col0-0", "col0-1", "col0-2", {}, "col0-4"};
    const ColDataType col1{"col1-0", {}, "col1-2", "col1-3", "col1-4"};
    const ColDataType col2{"col2-0", "col2-1", {}, "col2-3", "col2-4"};

    /// Prepare some names
    std::vector<String> col_names{"proj_col0", "proj_col1", "proj_col2"};
    const String db_name{"test_db"};
    const String table_name{"projection_test_table"};
};

TEST_F(ExecutorProjectionTestRunner, Projection)
try
{
    auto request = getRequest({col_names[2]});
    executeStreams(request, {toNullableVec<String>(col_names[2], col2)});

    request = getRequest({col_names[0], col_names[1]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[1], col1),});
    
    request = getRequest({col_names[0], col_names[1], col_names[2]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[1], col1),
                    toNullableVec<String>(col_names[2], col2)});
}
CATCH

} // namespace tests
} // namespace DB

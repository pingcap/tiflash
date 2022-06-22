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

class ExecutorTopNTestRunner : public DB::tests::ExecutorTest
{
public:
    using ColDataType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColumnWithData = std::vector<ColDataType>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                            {{col_name, TiDB::TP::TypeString}},
                            {toNullableVec<String>(col_name, col0)});
    }

    std::shared_ptr<tipb::DAGRequest> getRequest(const String & col_name, bool is_desc, int limit_num)
    {
        return context.scan(db_name, table_name).topN(col_name, is_desc, limit_num).build(context);
    }

    /// Prepare some names
    const String db_name{"test_db"};
    const String table_name{"topn_test_table"};
    const String col_name{"topn_col"};
    ColumnWithData col0{"col0-0", "col0-1", "col0-2", {}, "col0-4", {}, "col0-6", "col0-7"};
};

TEST_F(ExecutorTopNTestRunner, TopN)
try
{
    size_t col_data_num = col0.size();
    std::shared_ptr<tipb::DAGRequest> request;
    ColumnsWithTypeAndName expect_cols;
    bool is_desc;

    /// Check topn result with various parameters
    for (size_t i = 1; i <= 1; ++i)
    {
        is_desc = static_cast<bool>(i); /// Set descent or ascent
        if (is_desc)
            sort(col0.begin(), col0.end(), std::greater<ColDataType>()); /// Sort col0 for the following comparison
        else
            sort(col0.begin(), col0.end());

        for (size_t limit_num = 0; limit_num <= col_data_num + 3; limit_num++)        
        {
            request = getRequest(col_name, is_desc, limit_num);
            
            if (limit_num == 0 || limit_num > col_data_num)
                expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.end()))};
            else
                expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.begin() + limit_num))};

            executeStreams(request, expect_cols);
        }
    }
}
CATCH

} // namespace tests
} // namespace DB


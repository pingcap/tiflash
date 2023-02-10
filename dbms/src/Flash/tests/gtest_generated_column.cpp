// Copyright 2023 PingCAP, Ltd.
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
#include <Debug/MockExecutor/TableScanBinder.h>

namespace DB
{
namespace tests
{

class GeneratedColumnTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({"test_db", "test_table"},
                             {{"col_1", TiDB::TP::TypeString}, {"col_2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("col_1", {"banana"}),
                              toNullableVec<String>("col_2", {"apple"})});
    }
};

TEST_F(GeneratedColumnTestRunner, BasicTest)
try
{
    for (size_t i = 0; i < 100; ++i)
    {
        auto request = context
            .scan("test_db", "test_table")
            .exchangeSender(tipb::PassThrough)
            .build(context);

        // insertGeneratedColumnToTableScanDAGRequest(i, request);

        // Add generated column to table_scan tipb.
        executeAndAssertColumnsEqual(request,
                {toNullableVec<String>({"banana"}),
                toNullableVec<String>({"apple"})});
    }
}
CATCH
} // namespace tests
} // namespace DB

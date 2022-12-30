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

#include <Debug/MockStorage.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExecutorsWithDMTestRunner : public DB::tests::ExecutorTest
{
public:
    using ColStringType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColInt64Type = typename TypeTraits<Int64>::FieldType;

    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt64 = std::vector<ColInt64Type>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockDeltaMerge({"test_db", "t1"},
                                  {{"col0", TiDB::TP::TypeLongLong},
                                   {"col1", TiDB::TP::TypeString}},
                                  {{toVec<Int64>("col0", col0)},
                                   {toNullableVec<String>("col1", col1)}});
        context.mockStorage()->setUseDeltaMerge();
    }
    const ColumnWithString col1{"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"};
    const ColumnWithInt64 col0{0, 1, 2, 3, 4, 5, 6, 7};
};

TEST_F(ExecutorsWithDMTestRunner, Basic)
try
{
    auto request = context
                       .scan("test_db", "t1")
                       .build(context);
    executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}, {toNullableVec<String>("col1", col1)}});
}
CATCH

} // namespace tests
} // namespace DB

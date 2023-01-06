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

#include <DataTypes/DataTypeFactory.h>
#include <Debug/MockStorage.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <mutex>

namespace DB
{
namespace tests
{
using DecimalField64 = DecimalField<Decimal64>;

class MockStorageTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }

    // single column table
    const ColumnWithNullableString col1{"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"};
    const ColumnWithInt64 col0{0, 1, 2, 3, 4, 5, 6, 7};

    MockStorage mock_storage;
    ColumnWithTypeAndName column;
    ColumnsWithTypeAndName columns;
};

TEST_F(MockStorageTestRunner, DeltaMergeStorageBasic)
try
{
    ColumnsWithTypeAndName columns{toVec<Int64>("col0", col0), toNullableVec<String>("col1", col1)};
    mock_storage.addTableSchemaForDeltaMerge("test", {{"col0", TiDB::TP::TypeLongLong}, {"col1", TiDB::TP::TypeString}});
    mock_storage.addTableDataForDeltaMerge(context.context, "test", columns);
    auto in = mock_storage.getStreamFromDeltaMerge(context.context, 1);

    ASSERT_INPUTSTREAM_BLOCK_UR(
        in,
        Block(columns));

    mock_storage.clear();
}
CATCH

TEST_F(MockStorageTestRunner, Decimal)
try
{
    auto type = DataTypeFactory::instance().get("Decimal(15,5)");
    column = createColumn<Decimal64>(
        std::make_tuple(15, 5),
        {DecimalField64(450256, 5)},
        "d1");
    columns.push_back(column);
    context.addMockTable({"test_db", "t"}, {{"d1", DataTypeFactory::instance().get("Decimal(15,5)")}}, columns);
    auto request = context.scan("test_db", "t").build(context);
    executeAndAssertColumnsEqual(request, columns);
}
CATCH
} // namespace tests
} // namespace DB

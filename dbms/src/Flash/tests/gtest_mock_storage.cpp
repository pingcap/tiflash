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

#include <DataTypes/DataTypeFactory.h>
#include <Debug/MockStorage.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
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

    struct DecimalTestData
    {
        size_t max_prec;
        size_t min_prec;
        size_t prec_interval;
        size_t scale_interval;
        String type;
    };

    size_t table_id = 1;

    std::vector<DecimalTestData> test_datas{{9, 1, 1, 2, "Decimal32"}, {18, 10, 2, 4, "Decimal64"}, {38, 19, 3, 6, "Decimal128"}, {65, 39, 4, 8, "Decimal256"}};

    static std::pair<ColumnWithTypeAndName, String> getColumnAndTypeName(size_t prec, size_t scale, bool nullable, const String & type)
    {
        ColumnWithTypeAndName column;
        String type_name;
        if (nullable)
        {
            type_name = fmt::format("Nullable(Decimal({},{}))", prec, scale);
            if (type == "Decimal32")
                column = createColumn<Nullable<Decimal32>>(std::make_tuple(prec, scale), {DecimalField32(450256, scale)}, "d1");
            else if (type == "Decimal64")
                column = createColumn<Nullable<Decimal64>>(std::make_tuple(prec, scale), {DecimalField64(450256, scale)}, "d1");
            else if (type == "Decimal128")
                column = createColumn<Nullable<Decimal128>>(std::make_tuple(prec, scale), {DecimalField128(450256, scale)}, "d1");
            else
            {
                Int256 val = 450256;
                column = createColumn<Nullable<Decimal256>>(std::make_tuple(prec, scale), {DecimalField256(val, scale)}, "d1");
            }
        }
        else
        {
            type_name = fmt::format("Decimal({},{})", prec, scale);
            if (type == "Decimal32")
                column = createColumn<Decimal32>(std::make_tuple(prec, scale), {DecimalField32(450256, scale)}, "d1");
            else if (type == "Decimal64")
                column = createColumn<Decimal64>(std::make_tuple(prec, scale), {DecimalField64(450256, scale)}, "d1");
            else if (type == "Decimal128")
                column = createColumn<Decimal128>(std::make_tuple(prec, scale), {DecimalField128(450256, scale)}, "d1");
            else
            {
                Int256 val = 450256;
                column = createColumn<Decimal256>(std::make_tuple(prec, scale), {DecimalField256(val, scale)}, "d1");
            }
        }

        return {column, type_name};
    }

    void testDecimalTable()
    {
        for (const auto & test_data : test_datas)
        {
            {
                for (size_t i = test_data.min_prec; i <= test_data.max_prec; i += test_data.prec_interval)
                {
                    for (size_t j = 1; j <= i && j <= 30; j += test_data.scale_interval)
                    {
                        ColumnWithTypeAndName column;
                        String type_name;
                        std::tie(column, type_name) = getColumnAndTypeName(i, j, i % 2, test_data.type);

                        auto table_name = "t" + std::to_string(table_id++);
                        context.addMockTable({"test_db", table_name}, {{"d1", DataTypeFactory::instance().get(type_name)}}, {column});
                        auto request = context.scan("test_db", table_name).build(context);
                        executeAndAssertColumnsEqual(request, {column}, true);
                    }
                }
            }
        }
    }
};

TEST_F(MockStorageTestRunner, DeltaMergeStorageBasic)
try
{
    ColumnsWithTypeAndName columns{toVec<Int64>("col0", col0), toNullableVec<String>("col1", col1)};
    mock_storage.addTableSchemaForDeltaMerge("test", {{"col0", TiDB::TP::TypeLongLong}, {"col1", TiDB::TP::TypeString}});
    mock_storage.addTableDataForDeltaMerge(context.context, "test", columns);
    auto in = mock_storage.getStreamFromDeltaMerge(context.context, 1);

    ASSERT_INPUTSTREAM_BLOCK_UR(in, Block(columns));
    mock_storage.clear();
}
CATCH

TEST_F(MockStorageTestRunner, Decimal)
try
{
    testDecimalTable();
}
CATCH
} // namespace tests
} // namespace DB

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

#include <Columns/countBytesInFilter.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>


namespace DB::tests
{

class DeserializeBinaryBulkTest
{
public:
    static IColumn::Filter createFilter(size_t limit, double filter_ratio)
    {
        IColumn::Filter filter;
        filter.reserve(limit);
        std::mt19937_64 rng(0);
        std::bernoulli_distribution dist(filter_ratio);
        for (size_t i = 0; i < limit; ++i)
            filter.push_back(dist(rng));
        return filter;
    }

    static ColumnPtr createColumn(const DataTypePtr & data_type, size_t limit)
    {
        return ColumnGenerator::instance().generate({limit, data_type->getName(), RANDOM, "EXAMPLE", 128}).column;
    }

    static void testDeserialize(const DataTypePtr & data_type, size_t limit, double filter_ratio)
    {
        auto column = createColumn(data_type, limit);
        const auto filter = createFilter(limit, filter_ratio);
        WriteBufferFromOwnString write_buf;
        data_type->serializeBinaryBulk(*column, write_buf, 0, limit);
        auto str = write_buf.releaseStr();
        {
            ReadBufferFromMemory read_buf(str.data(), str.size());

            auto dest_column = data_type->createColumn();
            data_type->deserializeBinaryBulk(*dest_column, read_buf, limit, 0, nullptr);
            ASSERT_COLUMN_EQ(column, std::move(dest_column));
        }
        {
            ReadBufferFromMemory read_buf(str.data(), str.size());
            auto dest_column = data_type->createColumn();
            size_t passed_count = countBytesInFilter(filter);
            data_type->deserializeBinaryBulk(*dest_column, read_buf, limit, 0, &filter);
            column = column->filter(filter, passed_count);
            ASSERT_COLUMN_EQ(column, std::move(dest_column));
        }
    }
};

TEST(DeserializeBinaryBulkTest, Simple)
try
{
    std::vector<String> all_types{
        "Int64",
        "Int32",
        "UInt64",
        "UInt32",
        "Decimal(5,2)",
        "Decimal(10,2)",
        "Decimal(20,2)",
        "Decimal(40,2)",
        "Enum8('a' = 0,'b' = 1,'c' = 2)",
        "Enum16('a' = 0,'b' = 1,'c' = 2)",
        "MyDate",
        "MyDateTime",
        "String",
        "FixedString(128)"};
    for (const auto & type_name : all_types)
    {
        auto data_type = DataTypeFactory::instance().get(type_name);
        DeserializeBinaryBulkTest::testDeserialize(data_type, 8 * 1024, 0);
        DeserializeBinaryBulkTest::testDeserialize(data_type, 8 * 1024, 0.1);
        DeserializeBinaryBulkTest::testDeserialize(data_type, 8 * 1024, 0.5);
        DeserializeBinaryBulkTest::testDeserialize(data_type, 8 * 1024, 0.9);
        DeserializeBinaryBulkTest::testDeserialize(data_type, 8 * 1024, 1.0);
    }
}
CATCH

} // namespace DB::tests

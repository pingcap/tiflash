// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/sortBlock.h>
#include <TestUtils/FunctionTestUtils.h>


namespace DB
{
namespace tests
{

class HashJoinKeyTest : public ::testing::Test
{
public:
    template <typename ColumnType, typename... TParms>
    void testOneNumber(TParms &&... parms)
    {
        using T = ColumnType::value_type;
        TiDB::TiDBCollators collators;
        HashJoinKeyOneNumber<T> key_getter(collators);
        typename ColumnType::MutablePtr column = ColumnType::create(std::forward<TParms>(parms)...);
        const size_t n = 64;
        for (size_t i = 0; i < n; ++i)
            column->insert(static_cast<T>(i));
        ColumnRawPtrs key_columns{column.get()};
        key_getter.reset(key_columns, 0);
        ASSERT_EQ(key_getter.getRequiredKeyOffset(static_cast<T>(0)), sizeof(T));
        key_getter.reset(key_columns, 1);
        ASSERT_EQ(key_getter.getRequiredKeyOffset(static_cast<T>(0)), 0);
        union
        {
            T key;
            char data[sizeof(T)]{};
        };
        size_t offset = 0;
        PaddedPODArray<char> serialized_data(n * sizeof(T));
        PaddedPODArray<char *> pos(n);
        for (size_t i = 0; i < n; ++i)
        {
            ASSERT_EQ(key_getter.getJoinKey(i), static_cast<T>(i));
            ASSERT_EQ(key_getter.getJoinKeyWithBuffer(i), static_cast<T>(i));
            ASSERT_EQ(key_getter.getJoinKeyByteSize(static_cast<T>(i)), sizeof(T));
            pos[i] = &serialized_data[offset];
            offset += sizeof(T);
            key_getter.serializeJoinKey(static_cast<T>(i), pos[i]);
            ASSERT_EQ(key_getter.deserializeJoinKey(pos[i]), static_cast<T>(i));
        }
        typename ColumnType::MutablePtr new_column = ColumnType::create(std::forward<TParms>(parms)...);
        new_column->deserializeAndInsertFromPos(pos, true);
        new_column->flushNTAlignBuffer();
        ASSERT_COLUMN_EQ(std::move(column), std::move(new_column));
    }
};

TEST_F(HashJoinKeyTest, OneKey)
try
{
    testOneNumber<ColumnVector<UInt8>>();
    testOneNumber<ColumnVector<Int16>>();
    testOneNumber<ColumnVector<UInt32>>();
    testOneNumber<ColumnVector<Int64>>();
    testOneNumber<ColumnDecimal<Decimal32>>(0, 1);
    testOneNumber<ColumnDecimal<Decimal64>>(0, 2);
    testOneNumber<ColumnDecimal<Decimal128>>(0, 3);
}
CATCH

} // namespace tests
} // namespace DB

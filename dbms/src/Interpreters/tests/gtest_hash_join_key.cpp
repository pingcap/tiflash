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
    void testOneKeyNumber(TParms &&... parms)
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
        PaddedPODArray<char> serialized_data(n * sizeof(T));
        PaddedPODArray<char *> pos(n);
        for (size_t i = 0; i < n; ++i)
        {
            ASSERT_EQ(key_getter.getJoinKey(i), static_cast<T>(i));
            ASSERT_EQ(key_getter.getJoinKeyWithBuffer(i), static_cast<T>(i));
            ASSERT_EQ(key_getter.getJoinKeyByteSize(static_cast<T>(i)), sizeof(T));
            pos[i] = &serialized_data[i * sizeof(T)];
            key_getter.serializeJoinKey(static_cast<T>(i), pos[i]);
            ASSERT_EQ(key_getter.deserializeJoinKey(pos[i]), static_cast<T>(i));
        }
        typename ColumnType::MutablePtr new_column = ColumnType::create(std::forward<TParms>(parms)...);
        new_column->deserializeAndInsertFromPos(pos, true);
        new_column->flushNTAlignBuffer();
        ASSERT_COLUMN_EQ(std::move(column), std::move(new_column));
    }

    template <bool padding>
    void testOneKeyStringBin()
    {
        std::vector<std::string> data
            = {"abcd", "1234 ", "a1b2c3d4", "", "dasfderw123489f8dayffdasdfcs32q234fd", "dafsd sdfa   "};
        size_t n = data.size();
        auto string_column = ColumnString::create();
        for (auto & s : data)
            string_column->insert(s);

        TiDB::TiDBCollators collators;
        HashJoinKeyStringBin<padding> key_getter(collators);
        ColumnRawPtrs key_columns{string_column.get()};
        key_getter.reset(key_columns, 0);
        ASSERT_EQ(
            key_getter.getRequiredKeyOffset(key_getter.getJoinKey(0)),
            key_getter.getJoinKeyByteSize(key_getter.getJoinKey(0)));
        if constexpr (!padding)
        {
            key_getter.reset(key_columns, 1);
            ASSERT_EQ(key_getter.getRequiredKeyOffset(key_getter.getJoinKey(0)), 0);
        }

        PaddedPODArray<char> serialized_data(string_column->byteSize());
        size_t offset = 0;
        PaddedPODArray<char *> pos(n);
        for (size_t i = 0; i < n; ++i)
        {
            auto join_key = key_getter.getJoinKey(i);
            ASSERT_EQ(key_getter.getJoinKeyWithBuffer(i), join_key);
            size_t sz = key_getter.getJoinKeyByteSize(join_key);
            ASSERT_EQ(sz, sizeof(UInt32) + join_key.size);
            pos[i] = &serialized_data[offset];
            offset += sz;
            key_getter.serializeJoinKey(join_key, pos[i]);
            ASSERT_EQ(key_getter.deserializeJoinKey(pos[i]), join_key);
        }
        ASSERT_TRUE(offset <= string_column->byteSize());

        if constexpr (!padding)
        {
            auto new_column = ColumnString::create();
            new_column->deserializeAndInsertFromPos(pos, true);
            new_column->flushNTAlignBuffer();
            ASSERT_COLUMN_EQ(std::move(string_column), std::move(new_column));
        }
    }
};

TEST_F(HashJoinKeyTest, OneKeyNumber)
try
{
    testOneKeyNumber<ColumnVector<UInt8>>();
    testOneKeyNumber<ColumnVector<Int16>>();
    testOneKeyNumber<ColumnVector<UInt32>>();
    testOneKeyNumber<ColumnVector<Int64>>();
    testOneKeyNumber<ColumnDecimal<Decimal32>>(0, 1);
    testOneKeyNumber<ColumnDecimal<Decimal64>>(0, 2);
    testOneKeyNumber<ColumnDecimal<Decimal128>>(0, 3);
}
CATCH

TEST_F(HashJoinKeyTest, KeysFixed)
try
{
    std::vector<std::pair<UInt32, Int16>> vec_data = {
        {10, -30},
        {20, 40},
        {30, -40},
        {1234567890, 32767},
        {4294967295, -32768},
    };
    size_t n = vec_data.size();
    auto vec1 = ColumnVector<UInt32>::create();
    auto vec2 = ColumnVector<Int16>::create();
    for (auto [d1, d2] : vec_data)
    {
        vec1->insert(d1);
        vec2->insert(d2);
    }
    TiDB::TiDBCollators collators;
    HashJoinKeysFixed<UInt64> key_getter(collators);
    ColumnRawPtrs key_columns{vec1.get(), vec2.get()};
    key_getter.reset(key_columns, 0);
    ASSERT_EQ(key_getter.getRequiredKeyOffset(0), sizeof(UInt64));
    key_getter.reset(key_columns, 1);
    ASSERT_EQ(key_getter.getRequiredKeyOffset(0), sizeof(UInt64) - sizeof(Int16));

    ASSERT_EQ(key_getter.joinKeyIsEqual(key_getter.getJoinKeyWithBuffer(0), key_getter.getJoinKeyWithBuffer(0)), true);
    ASSERT_EQ(key_getter.joinKeyIsEqual(key_getter.getJoinKeyWithBuffer(0), key_getter.getJoinKeyWithBuffer(1)), false);

    PaddedPODArray<char> serialized_data(n * sizeof(UInt64));
    PaddedPODArray<char *> pos(n);
    union
    {
        UInt64 join_key;
        char key_data[sizeof(UInt64)]{};
    };
    size_t offset = sizeof(UInt64) - sizeof(UInt32) - sizeof(Int16);
    for (size_t i = 0; i < n; ++i)
    {
        std::memcpy(&key_data[offset], &vec1->getElement(i), sizeof(UInt32));
        std::memcpy(&key_data[offset + sizeof(UInt32)], &vec2->getElement(i), sizeof(Int16));
        ASSERT_EQ(key_getter.getJoinKey(i), join_key);
        ASSERT_EQ(key_getter.getJoinKeyWithBuffer(i), join_key);
        ASSERT_EQ(key_getter.getJoinKeyByteSize(join_key), sizeof(UInt64));
        key_getter.serializeJoinKey(join_key, &serialized_data[i * sizeof(UInt64)]);
        ASSERT_EQ(join_key, key_getter.deserializeJoinKey(&serialized_data[i * sizeof(UInt64)]));
        pos[i] = &serialized_data[i * sizeof(UInt64)] + key_getter.getRequiredKeyOffset(join_key);
    }
    auto new_column = ColumnVector<Int16>::create();
    new_column->deserializeAndInsertFromPos(pos, true);
    new_column->flushNTAlignBuffer();
    ASSERT_COLUMN_EQ(std::move(vec2), std::move(new_column));
}
CATCH

TEST_F(HashJoinKeyTest, KeysFixedOther)
try
{
    std::vector<std::tuple<Int32, Int64, UInt64, Decimal128>> vec_data = {
        {10, 20, 30, 40},
        {-10, -20, 30, -40},
        {40, -30, 20, -10},
        {1234567890, -1234567890, 1234567890000, -1234567890},
        {-1234567890, 1234567890, 1234567890000, 1234567890},
    };
    size_t n = vec_data.size();
    auto vec1 = ColumnVector<Int32>::create();
    auto vec2 = ColumnVector<Int64>::create();
    auto vec3 = ColumnVector<UInt64>::create();
    auto vec4 = ColumnDecimal<Decimal128>::create(0, 6);
    for (auto [d1, d2, d3, d4] : vec_data)
    {
        vec1->insert(d1);
        vec2->insert(d2);
        vec3->insert(d3);
        vec4->insert(d4);
    }
    TiDB::TiDBCollators collators;
    HashJoinKeysFixedOther key_getter(collators);
    ColumnRawPtrs key_columns{vec1.get(), vec2.get(), vec3.get(), vec4.get()};
    key_getter.reset(key_columns, 0);
    constexpr size_t total_size = sizeof(Int32) + sizeof(Int64) + sizeof(UInt64) + sizeof(Decimal128);
    ASSERT_EQ(key_getter.getRequiredKeyOffset(key_getter.getJoinKey(0)), total_size);
    key_getter.reset(key_columns, 2);
    ASSERT_EQ(key_getter.getRequiredKeyOffset(key_getter.getJoinKey(0)), sizeof(Int32) + sizeof(Int64));

    ASSERT_EQ(key_getter.joinKeyIsEqual(key_getter.getJoinKeyWithBuffer(0), key_getter.getJoinKeyWithBuffer(0)), true);
    ASSERT_EQ(key_getter.joinKeyIsEqual(key_getter.getJoinKeyWithBuffer(0), key_getter.getJoinKeyWithBuffer(1)), false);

    PaddedPODArray<char> serialized_data(n * total_size);
    PaddedPODArray<char *> pos(n);
    char key_data[total_size];
    StringRef join_key(key_data, total_size);
    for (size_t i = 0; i < n; ++i)
    {
        std::memcpy(&key_data[0], &vec1->getElement(i), sizeof(Int32));
        std::memcpy(&key_data[sizeof(Int32)], &vec2->getElement(i), sizeof(Int64));
        std::memcpy(&key_data[sizeof(Int32) + sizeof(Int64)], &vec3->getElement(i), sizeof(UInt64));
        std::memcpy(
            &key_data[sizeof(Int32) + sizeof(Int64) + sizeof(UInt64)],
            &vec4->getElement(i),
            sizeof(Decimal128));
        ASSERT_EQ(key_getter.getJoinKey(i), join_key);
        ASSERT_EQ(key_getter.getJoinKeyWithBuffer(i), join_key);
        ASSERT_EQ(key_getter.getJoinKeyByteSize(join_key), total_size);
        key_getter.serializeJoinKey(join_key, &serialized_data[i * total_size]);
        ASSERT_EQ(join_key, key_getter.deserializeJoinKey(&serialized_data[i * total_size]));
        pos[i] = &serialized_data[i * total_size] + key_getter.getRequiredKeyOffset(join_key);
    }
    auto new_column = ColumnVector<UInt64>::create();
    auto new_column2 = ColumnDecimal<Decimal128>::create(0, 6);
    new_column->deserializeAndInsertFromPos(pos, true);
    new_column->flushNTAlignBuffer();
    new_column2->deserializeAndInsertFromPos(pos, true);
    new_column2->flushNTAlignBuffer();
    ASSERT_COLUMN_EQ(std::move(vec3), std::move(new_column));
    ASSERT_COLUMN_EQ(std::move(vec4), std::move(new_column2));
}
CATCH

TEST_F(HashJoinKeyTest, KeyStringBin)
try
{
    testOneKeyStringBin<false>();
    testOneKeyStringBin<true>();
}
CATCH

} // namespace tests
} // namespace DB

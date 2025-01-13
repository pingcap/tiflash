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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnTuple.h>
#include <Common/COWPtr.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestColumnSerializeDeserialize : public ::testing::Test
{
public:
    static void testCountSerializeByteSize(
        const ColumnPtr & column_ptr,
        const PaddedPODArray<size_t> & result_byte_size,
        bool for_compare = false,
        const TiDB::TiDBCollatorPtr & collator = nullptr)
    {
        PaddedPODArray<size_t> byte_size;
        byte_size.resize_fill_zero(column_ptr->size());
        for (size_t i = 0; i < column_ptr->size(); ++i)
            byte_size[i] = i;
        if (!for_compare)
            column_ptr->countSerializeByteSize(byte_size);
        else
            column_ptr->countSerializeByteSizeForCmp(byte_size, collator);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
        {
            ASSERT_EQ(byte_size[i], i + result_byte_size[i]);
        }
    }

    static void testCountSerializeByteSizeForColumnArray(
        const ColumnPtr & column_ptr,
        const ColumnPtr & offsets,
        const PaddedPODArray<size_t> & result_byte_size,
        bool for_compare = false,
        const TiDB::TiDBCollatorPtr & collator = nullptr)
    {
        auto column_array = ColumnArray::create(column_ptr->cloneFullColumn(), offsets->cloneFullColumn());
        PaddedPODArray<size_t> byte_size;
        byte_size.resize_fill_zero(column_array->size());
        for (size_t i = 0; i < column_array->size(); ++i)
            byte_size[i] = i;
        if (!for_compare)
            column_array->countSerializeByteSize(byte_size);
        else
            column_array->countSerializeByteSizeForCmp(byte_size, collator);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
            ASSERT_EQ(byte_size[i], sizeof(UInt32) + i + result_byte_size[i]);
    }

    static void checkForColumnWithCollator(
        const ColumnPtr & result_col_ptr,
        const ColumnPtr & new_col_ptr,
        const TiDB::TiDBCollatorPtr & collator)
    {
        ASSERT_TRUE(collator);
        String sort_key_container;
        ASSERT_EQ(result_col_ptr->size(), new_col_ptr->size());
        // Special handling for ColumnTuple and ColumnArray.
        if (result_col_ptr->getFamilyName() == String("Array"))
        {
            size_t null_row_idx = 0;
            for (size_t i = 0; i < result_col_ptr->size(); ++i)
            {
                const auto & expected_inner_col = checkAndGetColumn<ColumnArray>(result_col_ptr.get())->getData();
                const auto & actual_inner_col = checkAndGetColumn<ColumnArray>(new_col_ptr.get())->getData();

                Field expected_arr_field;
                result_col_ptr->get(i, expected_arr_field);
                auto expected_arr = expected_arr_field.get<Array>();

                Field actual_arr_field;
                new_col_ptr->get(i, actual_arr_field);
                auto actual_arr = actual_arr_field.get<Array>();

                ASSERT_EQ(expected_arr.size(), actual_arr.size());

                for (size_t j = 0; j < expected_arr.size(); ++j, null_row_idx++)
                {
                    ASSERT_EQ(expected_inner_col.isNullAt(null_row_idx), actual_inner_col.isNullAt(null_row_idx));
                    if (expected_inner_col.isNullAt(null_row_idx))
                        continue;

                    auto expected_str = expected_arr[j].get<String>();
                    auto sort_key = collator->sortKey(expected_str.data(), expected_str.size(), sort_key_container);

                    const auto & actual_str = actual_arr[j].get<String>();
                    ASSERT_TRUE(sort_key == actual_str);
                }
            }
        }
        else if (result_col_ptr->getFamilyName() == String("Tuple"))
        {
            // getDataAt() not impl for ColumnTuple
            ASSERT_EQ(result_col_ptr->size(), new_col_ptr->size());
            for (size_t i = 0; i < result_col_ptr->size(); ++i)
            {
                const auto & expected_inner_col = checkAndGetColumn<ColumnTuple>(result_col_ptr.get())->getColumns()[0];
                const auto & actual_inner_col = checkAndGetColumn<ColumnTuple>(new_col_ptr.get())->getColumns()[0];

                ASSERT_EQ(expected_inner_col->isNullAt(i), actual_inner_col->isNullAt(i));
                if (expected_inner_col->isNullAt(i))
                    continue;

                auto expected_tuple_field = (*result_col_ptr)[i];
                const auto & expected_tuple = expected_tuple_field.get<Tuple>().toUnderType();

                auto actual_tuple_field = (*new_col_ptr)[i];
                const auto & actual_tuple = actual_tuple_field.get<Tuple>().toUnderType();

                ASSERT_EQ(expected_tuple.size(), actual_tuple.size());

                for (size_t j = 0; j < expected_tuple.size(); ++j)
                {
                    if (checkAndGetColumn<ColumnTuple>(result_col_ptr.get())->getColumns()[j]->getFamilyName()
                        == String("String"))
                    {
                        auto res = expected_tuple[j].get<String>();
                        auto sort_key = collator->sortKey(res.data(), res.size(), sort_key_container);

                        const auto & actual_str = actual_tuple[j].get<String>();
                        ASSERT_TRUE(sort_key == actual_str);
                    }
                    else
                    {
                        ASSERT_TRUE(expected_tuple[j] == actual_tuple[j]);
                    }
                }
            }
        }
        else
        {
            for (size_t i = 0; i < result_col_ptr->size(); ++i)
            {
                ASSERT_EQ(result_col_ptr->isNullAt(i), new_col_ptr->isNullAt(i));
                if (result_col_ptr->isNullAt(i))
                    continue;
                auto res = result_col_ptr->getDataAt(i);
                auto sort_key = collator->sortKey(res.data, res.size, sort_key_container);
                ASSERT_TRUE(sort_key == new_col_ptr->getDataAt(i));
            }
        }
    }

    static void testSerializeAndDeserialize(
        const ColumnPtr & column_ptr,
        bool for_compare = false,
        const TiDB::TiDBCollatorPtr & collator = nullptr,
        String * sort_key_container = nullptr)
    {
        doTestSerializeAndDeserialize(column_ptr, false, for_compare, collator, sort_key_container);
        doTestSerializeAndDeserialize2(column_ptr, false, for_compare, collator, sort_key_container);
        doTestSerializeAndDeserialize(column_ptr, true, for_compare, collator, sort_key_container);
        doTestSerializeAndDeserialize2(column_ptr, true, for_compare, collator, sort_key_container);
    }

    static void doTestSerializeAndDeserialize(
        const ColumnPtr & column_ptr,
        bool use_nt_align_buffer,
        bool for_compare = false,
        const TiDB::TiDBCollatorPtr & collator = nullptr,
        String * sort_key_container = nullptr)
    {
        PaddedPODArray<size_t> byte_size;
        byte_size.resize_fill_zero(column_ptr->size());
        if (!for_compare)
            column_ptr->countSerializeByteSize(byte_size);
        else
            column_ptr->countSerializeByteSizeForCmp(byte_size, collator);
        size_t total_size = 0;
        for (const auto size : byte_size)
            total_size += size;
        PaddedPODArray<char> memory(total_size);
        PaddedPODArray<char *> pos;
        size_t current_size = 0;
        for (size_t i = 0; i < byte_size.size() / 2; ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        PaddedPODArray<char *> ori_pos;
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr->serializeToPos(pos, 0, byte_size.size() / 2, false);
        else
            column_ptr->serializeToPosForCmp(pos, 0, byte_size.size() / 2, false, collator, sort_key_container);

        auto new_col_ptr = column_ptr->cloneEmpty();
        if (use_nt_align_buffer)
            new_col_ptr->reserveAlign(byte_size.size(), FULL_VECTOR_SIZE_AVX2);
        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);

        current_size = 0;
        pos.clear();
        ori_pos.clear();
        for (size_t i = byte_size.size() / 2; i < byte_size.size() - 1; ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        pos.push_back(nullptr);
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr->serializeToPos(pos, byte_size.size() / 2, byte_size.size() - byte_size.size() / 2, true);
        else
            column_ptr->serializeToPosForCmp(
                pos,
                byte_size.size() / 2,
                byte_size.size() - byte_size.size() / 2,
                true,
                collator,
                sort_key_container);
        pos.resize(pos.size() - 1);
        ori_pos.resize(ori_pos.size() - 1);

        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);

        current_size = 0;
        pos.clear();
        ori_pos.clear();
        for (const auto size : byte_size)
        {
            pos.push_back(memory.data() + current_size);
            current_size += size;
        }
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr->serializeToPos(pos, 0, byte_size.size(), true);
        else
            column_ptr->serializeToPosForCmp(pos, 0, byte_size.size(), true, collator, sort_key_container);

        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);
        if (use_nt_align_buffer)
            new_col_ptr->flushNTAlignBuffer();

        auto result_col_ptr = column_ptr->cloneFullColumn();
        result_col_ptr->popBack(1);
        for (size_t i = 0; i < column_ptr->size(); ++i)
            result_col_ptr->insertFrom(*column_ptr, i);

        if (collator != nullptr)
            checkForColumnWithCollator(std::move(result_col_ptr), std::move(new_col_ptr), collator);
        else
            ASSERT_COLUMN_EQ(std::move(result_col_ptr), std::move(new_col_ptr));
    }

    static void doTestSerializeAndDeserialize2(
        const ColumnPtr & column_ptr,
        bool use_nt_align_buffer,
        bool for_compare = false,
        const TiDB::TiDBCollatorPtr & collator = nullptr,
        String * sort_key_container = nullptr)
    {
        if (column_ptr->size() < 2)
            return;
        PaddedPODArray<size_t> byte_size;
        byte_size.resize_fill_zero(column_ptr->size());
        if (!for_compare)
            column_ptr->countSerializeByteSize(byte_size);
        else
            column_ptr->countSerializeByteSizeForCmp(byte_size, collator);
        size_t total_size = 0;
        for (const auto size : byte_size)
            total_size += size;
        PaddedPODArray<char> memory(total_size);
        PaddedPODArray<char *> pos;
        PaddedPODArray<char *> ori_pos;
        size_t current_size = 0;
        for (size_t i = 0; i < byte_size.size() / 2 - 1; ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        pos.push_back(nullptr);
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr->serializeToPos(pos, 0, byte_size.size() / 2, true);
        else
            column_ptr->serializeToPosForCmp(pos, 0, byte_size.size() / 2, true, collator, sort_key_container);
        pos.resize(pos.size() - 1);
        ori_pos.resize(ori_pos.size() - 1);

        auto new_col_ptr = column_ptr->cloneEmpty();
        if (use_nt_align_buffer)
            new_col_ptr->reserveAlign(byte_size.size(), FULL_VECTOR_SIZE_AVX2);
        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);

        current_size = 0;
        pos.clear();
        ori_pos.clear();
        for (size_t i = byte_size.size() / 2 - 1; i < byte_size.size(); ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr
                ->serializeToPos(pos, byte_size.size() / 2 - 1, byte_size.size() - byte_size.size() / 2 + 1, false);
        else
            column_ptr->serializeToPosForCmp(
                pos,
                byte_size.size() / 2 - 1,
                byte_size.size() - byte_size.size() / 2 + 1,
                false,
                collator,
                sort_key_container);
        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);

        current_size = 0;
        pos.clear();
        ori_pos.clear();
        for (const auto size : byte_size)
        {
            pos.push_back(memory.data() + current_size);
            current_size += size;
        }
        for (auto * ptr : pos)
            ori_pos.push_back(ptr);
        if (!for_compare)
            column_ptr->serializeToPos(pos, 0, byte_size.size(), true);
        else
            column_ptr->serializeToPosForCmp(pos, 0, byte_size.size(), true, collator, sort_key_container);

        if (!for_compare)
            new_col_ptr->deserializeAndInsertFromPos(ori_pos, use_nt_align_buffer);
        else
            new_col_ptr->deserializeForCmpAndInsertFromPos(ori_pos, use_nt_align_buffer, collator);
        if (use_nt_align_buffer)
            new_col_ptr->flushNTAlignBuffer();

        auto result_col_ptr = column_ptr->cloneFullColumn();
        for (size_t i = 0; i < column_ptr->size(); ++i)
            result_col_ptr->insertFrom(*column_ptr, i);

        if (collator != nullptr)
            checkForColumnWithCollator(std::move(result_col_ptr), std::move(new_col_ptr), collator);
        else
            ASSERT_COLUMN_EQ(std::move(result_col_ptr), std::move(new_col_ptr));
    }
};

TEST_F(TestColumnSerializeDeserialize, TestColumnVector)
try
{
    auto col_vector_1 = createColumn<UInt32>({1}).column;
    testCountSerializeByteSize(col_vector_1, {4});
    testSerializeAndDeserialize(col_vector_1);
    testSerializeAndDeserialize(col_vector_1, true, nullptr, nullptr);

    auto col_vector = createColumn<UInt64>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}).column;
    testCountSerializeByteSize(col_vector, {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 10, 16, 18}).column;
    testCountSerializeByteSizeForColumnArray(col_vector, col_offsets, {8, 16, 24, 32, 48, 16});

    testSerializeAndDeserialize(col_vector);
    testSerializeAndDeserialize(col_vector, true, nullptr, nullptr);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnDecimal)
try
{
    auto col_decimal_1 = createColumn<Decimal128>(std::make_tuple(10, 3), {"1234567.333"}).column;
    testCountSerializeByteSize(col_decimal_1, {16});
    testSerializeAndDeserialize(col_decimal_1);
    testSerializeAndDeserialize(col_decimal_1, true, nullptr, nullptr);

    auto col_decimal = createColumn<Decimal32>(
                           std::make_tuple(8, 2),
                           {"-1.0", "2.2",   "3.33",  "4",     "5",    "6",    "7.7",  "8.8",  "9.9",  "10",   "11",
                            "12",   "-13.3", "14.4",  "-15.5", "16.2", "17",   "18.8", "19.9", "20.0", "21",   "22",
                            "23",   "24",    "-25.5", "26.6",  "27.7", "28.8", "29.9", "30.1", "31",   "32.5", "33.9"})
                           .column;
    testCountSerializeByteSize(col_decimal, {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                             4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 12, 33}).column;
    testCountSerializeByteSizeForColumnArray(col_decimal, col_offsets, {4, 8, 12, 6 * 4, 21 * 4});

    testSerializeAndDeserialize(col_decimal);
    testSerializeAndDeserialize(col_decimal, true, nullptr, nullptr);

    auto col_decimal_256 = createColumn<Decimal256>(
                               std::make_tuple(61, 4),
                               {"1.0",
                                "-2.2",
                                "333333333333333333333333333333333333333333333333333333333.33",
                                "-4",
                                "-999999999999999999999999999999999999999999999999999999999.99",
                                "6",
                                "7.7",
                                "8.8",
                                "-9.9",
                                "10",
                                "11",
                                "12",
                                "13.3",
                                "-1412384819234.444",
                                "15.5",
                                "16.2",
                                "17",
                                "18.8",
                                "-19.9",
                                "20.0",
                                "21",
                                "22",
                                "23",
                                "24",
                                "25.5",
                                "26.6",
                                "-27.7",
                                "28.8",
                                "-29.9",
                                "30.1",
                                "31",
                                "32.5",
                                "-33.9999"})
                               .column;
    testCountSerializeByteSize(col_decimal_256, {48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                                                 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48});
    testCountSerializeByteSizeForColumnArray(col_decimal_256, col_offsets, {48, 2 * 48, 3 * 48, 6 * 48, 21 * 48});

    testSerializeAndDeserialize(col_decimal_256);
    testSerializeAndDeserialize(col_decimal_256, true, nullptr, nullptr);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnString)
try
{
    String sort_key_container;
    TiDB::TiDBCollatorPtr collator_utf8_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    TiDB::TiDBCollatorPtr collator_utf8_general_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    TiDB::TiDBCollatorPtr collator_utf8_unicode_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);

    auto col_string_1 = createColumn<String>({"sdafyuwer123"}).column;
    testCountSerializeByteSize(col_string_1, {4 + 13});
    testSerializeAndDeserialize(col_string_1);
    testSerializeAndDeserialize(col_string_1, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_string_1, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_string_1, true, collator_utf8_unicode_ci, &sort_key_container);

    auto col_string = createColumn<String>({"123",
                                            "1234567890",
                                            "4567",
                                            "-234567890",
                                            "8901",
                                            "1234567890",
                                            "123456789012",
                                            "234567",
                                            "12345678",
                                            "123456",
                                            "123456789012",
                                            "12345678901234567",
                                            "12345678901234",
                                            "123456789012",
                                            "123456789",
                                            "12345678901234567890"})
                          .column;
    testCountSerializeByteSize(
        col_string,
        {4 + 4,
         4 + 11,
         4 + 5,
         4 + 11,
         4 + 5,
         4 + 11,
         4 + 13,
         4 + 7,
         4 + 9,
         4 + 7,
         4 + 13,
         4 + 18,
         4 + 15,
         4 + 13,
         4 + 10,
         4 + 21});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 10, 16}).column;
    testCountSerializeByteSizeForColumnArray(col_string, col_offsets, {4 + 4, 8 + 16, 12 + 27, 16 + 36, 24 + 90});

    testSerializeAndDeserialize(col_string);

    testSerializeAndDeserialize(col_string, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_string, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_string, true, collator_utf8_unicode_ci, &sort_key_container);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnFixedString)
try
{
    auto col_fixed_string_mut = ColumnFixedString::create(2);
    col_fixed_string_mut->insertData("a", 1);
    col_fixed_string_mut->insertData("b", 1);
    col_fixed_string_mut->insertData("c", 1);
    col_fixed_string_mut->insertData("d", 1);
    col_fixed_string_mut->insertData("e", 1);
    col_fixed_string_mut->insertData("ff", 2);
    ColumnPtr col_fixed_string = std::move(col_fixed_string_mut);
    testCountSerializeByteSize(col_fixed_string, {2, 2, 2, 2, 2, 2});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerializeByteSizeForColumnArray(col_fixed_string, col_offsets, {2, 4, 6});

    testSerializeAndDeserialize(col_fixed_string);
    // ColumnFixedString doesn't support serialize/deserialize with collator for now.
    testSerializeAndDeserialize(col_fixed_string, true, nullptr, nullptr);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnNullable)
try
{
    // ColumnNullable(ColumnDecimal)
    auto col_nullable_vec1
        = createNullableColumn<Decimal256>(std::make_tuple(65, 0), {"123456789012345678901234567890"}, {1}).column;
    testCountSerializeByteSize(col_nullable_vec1, {49});
    testSerializeAndDeserialize(col_nullable_vec1);
    testSerializeAndDeserialize(col_nullable_vec1, true, nullptr, nullptr);

    // ColumnNullable(ColumnVector)
    auto col_nullable_vec = createNullableColumn<UInt64>({1, 2, 3, 4, 5, 6}, {0, 1, 0, 1, 0, 1}).column;
    testCountSerializeByteSize(col_nullable_vec, {9, 9, 9, 9, 9, 9});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerializeByteSizeForColumnArray(col_nullable_vec, col_offsets, {9, 18, 27});
    testSerializeAndDeserialize(col_nullable_vec);
    testSerializeAndDeserialize(col_nullable_vec, true, nullptr, nullptr);

    // ColumnNullable(ColumnString)
    String sort_key_container;
    TiDB::TiDBCollatorPtr collator_utf8_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    TiDB::TiDBCollatorPtr collator_utf8_general_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    TiDB::TiDBCollatorPtr collator_utf8_unicode_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    auto col_nullable_string
        = createNullableColumn<String>({"123", "2", "34", "456", "5678", "6"}, {0, 1, 0, 1, 0, 1}).column;
    testCountSerializeByteSize(col_nullable_string, {5 + 4, 5 + 1, 5 + 3, 5 + 1, 5 + 5, 5 + 1});
    testCountSerializeByteSizeForColumnArray(col_nullable_string, col_offsets, {5 + 4, 10 + 4, 15 + 7});
    testSerializeAndDeserialize(col_nullable_string);
    testSerializeAndDeserialize(col_nullable_string, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_nullable_string, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_nullable_string, true, collator_utf8_unicode_ci, &sort_key_container);

    // ColumnNullable(ColumnArray(ColumnVector))
    auto col_vector = createColumn<Float32>({1.0, 2.2, 3.3, 4.4, 5.5, 6.1}).column;
    auto col_array_vec = ColumnArray::create(col_vector, col_offsets);
    auto col_nullable_array_vec = ColumnNullable::create(col_array_vec, createColumn<UInt8>({1, 1, 1}).column);
    testCountSerializeByteSize(col_nullable_array_vec, {1 + 4 + 4, 1 + 4 + 8, 1 + 4 + 12});
    testSerializeAndDeserialize(col_nullable_array_vec);
    testSerializeAndDeserialize(col_nullable_array_vec, true, nullptr, nullptr);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnArray)
try
{
    // ColumnArray(ColumnVector)
    auto col_vector = createColumn<Float32>({1.0, 2.2, 3.3, 4.4, 5.5, 6.1}).column;
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    auto col_array_vec = ColumnArray::create(col_vector, col_offsets);
    testCountSerializeByteSize(col_array_vec, {4 + 4, 4 + 8, 4 + 12});
    testSerializeAndDeserialize(col_array_vec);
    testSerializeAndDeserialize(col_array_vec, true, nullptr, nullptr);

    // ColumnArray(ColumnString)
    String sort_key_container;
    TiDB::TiDBCollatorPtr collator_utf8_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    TiDB::TiDBCollatorPtr collator_utf8_general_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    TiDB::TiDBCollatorPtr collator_utf8_unicode_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    auto col_string = createColumn<String>({"123", "2", "34", "456", "5678", "6"}).column;
    auto col_array_string = ColumnArray::create(col_string, col_offsets);
    testCountSerializeByteSize(col_array_string, {4 + 4 + 4, 4 + 8 + 5, 4 + 12 + 11});
    testSerializeAndDeserialize(col_array_string);
    testSerializeAndDeserialize(col_array_string, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_array_string, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_array_string, true, collator_utf8_unicode_ci, &sort_key_container);

    // ColumnArray(ColumnNullable(ColumnString))
    auto col_nullable_string
        = createNullableColumn<String>({"123", "2", "34", "456", "5678", "6"}, {0, 1, 0, 1, 0, 1}).column;
    auto col_array_nullable_string = ColumnArray::create(col_nullable_string, col_offsets);
    testCountSerializeByteSize(col_array_nullable_string, {4 + 5 + 4, 4 + 10 + 4, 4 + 15 + 7});
    testSerializeAndDeserialize(col_array_nullable_string);
    testSerializeAndDeserialize(col_array_nullable_string, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_array_nullable_string, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_array_nullable_string, true, collator_utf8_unicode_ci, &sort_key_container);

    // ColumnArray(ColumnDecimal)
    auto col_decimal_256 = createColumn<Decimal256>(
                               std::make_tuple(20, 4),
                               {"1.0",
                                "2.2",
                                "-3333333333333333.3333",
                                "-4567654867645846",
                                "5",
                                "6",
                                "7.7",
                                "1232148.8",
                                "9.9",
                                "12341210",
                                "11",
                                "567612",
                                "-13.3",
                                "8745614.4557",
                                "15.5",
                                "16.2",
                                "-17",
                                "18.8",
                                "19.9",
                                "20.0",
                                "21",
                                "22",
                                "-23",
                                "24",
                                "25122412234.5",
                                "26.6",
                                "27.7",
                                "-1911239401927328.8999",
                                "29.9",
                                "30.1",
                                "31",
                                "32.5",
                                "33.9999"})
                               .column;
    auto col_offsets_decimal = createColumn<IColumn::Offset>({3, 8, 15, 20, 30, 31, 32, 33}).column;
    auto col_array_decimal_256 = ColumnArray::create(col_decimal_256, col_offsets_decimal);
    testCountSerializeByteSize(
        col_array_decimal_256,
        {4 + 3 * 48, 4 + 5 * 48, 4 + 7 * 48, 4 + 5 * 48, 4 + 10 * 48, 4 + 48, 4 + 48, 4 + 48});
    testSerializeAndDeserialize(col_array_decimal_256);
    testSerializeAndDeserialize(col_array_decimal_256, true, nullptr, nullptr);

    // ColumnArray(ColumnFixedString)
    auto col_fixed_string_mut = ColumnFixedString::create(2);
    col_fixed_string_mut->insertData("aa", 2);
    col_fixed_string_mut->insertData("bc", 2);
    col_fixed_string_mut->insertData("c", 1);
    col_fixed_string_mut->insertData("d", 1);
    col_fixed_string_mut->insertData("e1", 2);
    col_fixed_string_mut->insertData("ff", 2);
    ColumnPtr col_fixed_string = std::move(col_fixed_string_mut);
    auto col_array_fixed_string = ColumnArray::create(col_fixed_string, col_offsets);
    testCountSerializeByteSize(col_array_fixed_string, {4 + 2, 4 + 4, 4 + 6});
    testSerializeAndDeserialize(col_array_fixed_string);
    testSerializeAndDeserialize(col_array_fixed_string, true, nullptr, nullptr);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnTuple)
try
{
    auto col_tuple = ColumnTuple::create(
        {createColumn<UInt64>({1, 2, 3, 4, 5, 6}).column,
         createColumn<String>({"123", "2", "34", "456", "5678", "6"}).column});
    testCountSerializeByteSize(col_tuple, {8 + 4 + 4, 8 + 4 + 2, 8 + 4 + 3, 8 + 4 + 4, 8 + 4 + 5, 8 + 4 + 2});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerializeByteSizeForColumnArray(col_tuple, col_offsets, {8 + 4 + 4, 16 + 8 + 5, 24 + 12 + 11});

    testSerializeAndDeserialize(col_tuple);

    String sort_key_container;
    TiDB::TiDBCollatorPtr collator_utf8_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    TiDB::TiDBCollatorPtr collator_utf8_general_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    TiDB::TiDBCollatorPtr collator_utf8_unicode_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    testSerializeAndDeserialize(col_tuple, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_tuple, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_tuple, true, collator_utf8_unicode_ci, &sort_key_container);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnStringCollator)
try
{
    TiDB::TiDBCollatorPtr collator_utf8_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    TiDB::TiDBCollatorPtr collator_utf8_general_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    TiDB::TiDBCollatorPtr collator_utf8_unicode_ci
        = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);

    auto col_string = createColumn<String>({"hangzhou", "杭州", "你好世界", "欧元€", "abc里拉₤", "12法郎₣", ""}).column;
    testCountSerializeByteSize(
        col_string,
        {4 + 8, 4 + 6, 4 + 12, 4 + 9, 4 + 12, 4 + 11, 4 + 0},
        true,
        collator_utf8_bin);
    testCountSerializeByteSize(
        col_string,
        {4 + 8 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 2 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 4 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 3 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 6 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 5 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 + 0},
        true,
        collator_utf8_general_ci);
    testCountSerializeByteSize(
        col_string,
        {4 + 8 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 2 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 4 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 3 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 6 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 5 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 + 0},
        true,
        collator_utf8_unicode_ci);

    auto col_offset = createColumn<IColumn::Offset>({1, 4, 7}).column;
    testCountSerializeByteSizeForColumnArray(
        col_string,
        col_offset,
        {4 + 8 * collator_utf8_bin->maxBytesForOneChar(),
         4 * 3 + (6 + 12 + 9) * collator_utf8_bin->maxBytesForOneChar(),
         4 * 3 + (12 + 11) * collator_utf8_bin->maxBytesForOneChar()},
        true,
        collator_utf8_bin);
    testCountSerializeByteSizeForColumnArray(
        col_string,
        col_offset,
        {4 + 8 * collator_utf8_general_ci->maxBytesForOneChar(),
         4 * 3 + (2 + 4 + 3) * collator_utf8_general_ci->maxBytesForOneChar(),
         4 * 3 + (6 + 5) * collator_utf8_general_ci->maxBytesForOneChar()},
        true,
        collator_utf8_general_ci);
    testCountSerializeByteSizeForColumnArray(
        col_string,
        col_offset,
        {4 + 8 * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 * 3 + (2 + 4 + 3) * collator_utf8_unicode_ci->maxBytesForOneChar(),
         4 * 3 + (6 + 5) * collator_utf8_unicode_ci->maxBytesForOneChar()},
        true,
        collator_utf8_unicode_ci);

    String sort_key_container;
    testSerializeAndDeserialize(col_string, true, collator_utf8_bin, &sort_key_container);
    testSerializeAndDeserialize(col_string, true, collator_utf8_general_ci, &sort_key_container);
    testSerializeAndDeserialize(col_string, true, collator_utf8_unicode_ci, &sort_key_container);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestLargeColumnDecimal)
try
{
    std::vector<String> decimal_vals = {
        "999999999111111111",
        "999999999111111111",
        "999999999111111111",
        "12345678900123456789",
        "12345678900123456789",
    };
    auto col_dec256 = createColumn<Decimal256>(std::make_tuple(40, 6), decimal_vals).column;
    testSerializeAndDeserialize(col_dec256);
    testSerializeAndDeserialize(col_dec256, true, nullptr, nullptr);
}
CATCH

} // namespace tests
} // namespace DB

// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/NewHandleIndex.h>
#include <Storages/DeltaMerge/tests/gtest_segment_bitmap.h>
using namespace DB::tests;

namespace DB::DM::tests
{

class NewHandleIndexTest : public SegmentBitmapFilterTest
{
protected:
    void testInt64()
    {
        NewHandleIndex<Int64> handle_index;
        for (UInt32 i = 0; i <= 50; ++i)
            handle_index.insert(i * 2, i);

        // Insert duplicate handle is not allowed
        ASSERT_THROW(handle_index.insert(0, 100), DB::Exception);

        std::optional<DeltaValueReader> delta_reader;
        UInt32 stable_rows = 0;
        for (UInt32 i = 0; i <= 100; ++i)
        {
            auto row_id = handle_index.find(i, delta_reader, stable_rows);
            if (i % 2 == 0)
                ASSERT_EQ(row_id.value(), i / 2) << i;
            else
                ASSERT_FALSE(row_id.has_value()) << i;
        }

        // Boundary test
        handle_index.insert(std::numeric_limits<Int64>::max(), 1000);
        handle_index.insert(std::numeric_limits<Int64>::max() - 1, 1001);
        handle_index.insert(std::numeric_limits<Int64>::min(), 1002);
        handle_index.insert(std::numeric_limits<Int64>::min() + 1, 1003);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max(), delta_reader, stable_rows).value(), 1000);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max() - 1, delta_reader, stable_rows).value(), 1001);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min(), delta_reader, stable_rows).value(), 1002);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min() + 1, delta_reader, stable_rows).value(), 1003);

        // Delete range test
        const auto delete_range_normal = buildRowKeyRange(47, 134, is_common_handle);
        handle_index.deleteRange(delete_range_normal, delta_reader, stable_rows);
        for (Int64 i = 0; i <= 100; ++i)
        {
            auto row_id = handle_index.find(i, delta_reader, stable_rows);
            if (i % 2 == 0 && !inRowKeyRange(delete_range_normal, i))
                ASSERT_EQ(row_id.value(), i / 2) << i;
            else
                ASSERT_FALSE(row_id.has_value()) << i;
        }
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max(), delta_reader, stable_rows).value(), 1000);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max() - 1, delta_reader, stable_rows).value(), 1001);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min(), delta_reader, stable_rows).value(), 1002);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min() + 1, delta_reader, stable_rows).value(), 1003);

        const auto delete_range_right_bounary = buildRowKeyRange(
            std::numeric_limits<Int64>::max() - 1,
            std::numeric_limits<Int64>::max(),
            is_common_handle,
            /* including_right_boundary */ true);
        handle_index.deleteRange(delete_range_right_bounary, delta_reader, stable_rows);
        for (Int64 i = 0; i <= 100; ++i)
        {
            auto row_id = handle_index.find(i, delta_reader, stable_rows);
            if (i % 2 == 0 && !inRowKeyRange(delete_range_normal, i))
                ASSERT_EQ(row_id.value(), i / 2) << i;
            else
                ASSERT_FALSE(row_id.has_value()) << i;
        }
        ASSERT_FALSE(handle_index.find(std::numeric_limits<Int64>::max(), delta_reader, stable_rows).has_value());
        ASSERT_FALSE(handle_index.find(std::numeric_limits<Int64>::max() - 1, delta_reader, stable_rows).has_value());
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min(), delta_reader, stable_rows).value(), 1002);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::min() + 1, delta_reader, stable_rows).value(), 1003);

        handle_index.insert(std::numeric_limits<Int64>::max(), 1000);
        handle_index.insert(std::numeric_limits<Int64>::max() - 1, 1001);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max(), delta_reader, stable_rows).value(), 1000);
        ASSERT_EQ(handle_index.find(std::numeric_limits<Int64>::max() - 1, delta_reader, stable_rows).value(), 1001);
        const auto delete_range_all = RowKeyRange::newAll(is_common_handle, 1);
        handle_index.deleteRange(delete_range_all, delta_reader, stable_rows);
        ASSERT_TRUE(handle_index.handle_to_row_id.empty());
    }

    template <typename Hash>
    void testString()
    {
        // Create delta for NewHandleIndex<String>
        writeSegmentGeneric("d_tiny:[0, 66):shuffle:ts_1|d_tiny:[44, 107):shuffle:ts_2|d_tiny:[60, "
                            "160):shuffle:ts_3|d_tiny:[170, 171):shuffle:ts_4");
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        auto delta_rows = snap->delta->getRows();
        auto delta_reader = VersionChain<String>::createDeltaValueReaderIfCommonHandle(*dm_context, snap->delta);
        ASSERT_TRUE(delta_reader.has_value());
        UInt32 stable_rows = 0;

        MutableColumns mut_cols(1);
        mut_cols[0] = ColumnString::create();
        const auto read_rows = delta_reader->readRows(mut_cols, /*offset*/ 0, /*limit*/ delta_rows, /*range*/ nullptr);
        ASSERT_EQ(read_rows, delta_rows);
        ColumnView<String> handles(*(mut_cols[0]));

        std::map<std::string_view, UInt32> base_handle_to_row_id;
        std::multimap<std::string_view, UInt32> all_handle_to_row_id;
        for (UInt32 i = 0; i < handles.size(); ++i)
        {
            auto handle = handles[i];
            if (!base_handle_to_row_id.contains(handle))
                base_handle_to_row_id.emplace(handle, i + stable_rows);

            all_handle_to_row_id.emplace(handle, i + stable_rows);
        }

        NewHandleIndex<String, Hash> handle_index;
        for (UInt32 i = 0; i < handles.size(); ++i)
        {
            auto handle = handles[i];
            auto row_id = handle_index.find(handle, delta_reader, stable_rows);
            if (row_id.has_value())
                continue;

            handle_index.insert(handle, i + stable_rows);
        }

        for (UInt32 i = 0; i < handles.size(); ++i)
        {
            auto handle = handles[i];
            auto expected_row_id = base_handle_to_row_id.find(handle)->second;
            auto actual_row_id = handle_index.find(handle, delta_reader, stable_rows);
            ASSERT_EQ(actual_row_id.value(), expected_row_id) << i;
        }

        for (Int64 i = 160; i < 170; ++i)
        {
            auto handle = genMockCommonHandle(i, 1);
            auto row_id = handle_index.find(handle, delta_reader, stable_rows);
            ASSERT_FALSE(row_id.has_value()) << i;
        }

        const auto delete_range = buildRowKeyRange(47, 134, is_common_handle);
        handle_index.deleteRange(delete_range, delta_reader, stable_rows);
        for (UInt32 i = 0; i < handles.size(); ++i)
        {
            auto handle = handles[i];
            if (inRowKeyRange(delete_range, handle))
            {
                auto row_id = handle_index.find(handle, delta_reader, stable_rows);
                ASSERT_FALSE(row_id.has_value()) << i;
            }
            else
            {
                auto expected_row_id = base_handle_to_row_id.find(handle)->second;
                auto actual_row_id = handle_index.find(handle, delta_reader, stable_rows);
                ASSERT_EQ(actual_row_id.value(), expected_row_id) << i;
            }
        }
    }

    struct HighCollisionHash
    {
        size_t operator()(std::string_view handle) const { return decodeMockCommonHandle(String(handle)) % 3; }
    };
};

INSTANTIATE_TEST_CASE_P(VersionChain, NewHandleIndexTest, /* is_common_handle */ ::testing::Bool());

TEST_P(NewHandleIndexTest, Normal)
try
{
    if (is_common_handle)
    {
        // The default hash type.
        testString<absl::Hash<std::string_view>>();
        // Use a hash function with high collision rate.
        testString<NewHandleIndexTest::HighCollisionHash>();
    }
    else
        testInt64();
}
CATCH

} // namespace DB::DM::tests

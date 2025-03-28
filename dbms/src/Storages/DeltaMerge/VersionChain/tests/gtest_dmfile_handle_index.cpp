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

#include <Storages/DeltaMerge/tests/gtest_segment_bitmap.h>

using namespace DB::tests;

namespace DB::DM::tests
{

class DMFileHandleIndexTest : public SegmentBitmapFilterTest
{
protected:
    template <ExtraHandleType HandleType>
    std::vector<HandleType> genHandles(const std::vector<Int64> & int_handles)
    {
        if constexpr (!isCommonHandle<HandleType>())
            return int_handles;
        else
        {
            std::vector<HandleType> handles;
            handles.reserve(int_handles.size());
            for (Int64 int_handle : int_handles)
                handles.emplace_back(genMockCommonHandle(int_handle, 1));
            return handles;
        }
    }

    template <ExtraHandleType HandleType>
    std::vector<std::vector<HandleType>> genHandlePacks(
        const std::vector<std::vector<Int64>> & expected_int_handle_packs)
    {
        if constexpr (!isCommonHandle<HandleType>())
            return expected_int_handle_packs;
        else
        {
            std::vector<std::vector<HandleType>> expected_handle_packs;
            for (const auto & int_handle_pack : expected_int_handle_packs)
                expected_handle_packs.emplace_back(genHandles<HandleType>(int_handle_pack));
            return expected_handle_packs;
        }
    }

    template <ExtraHandleType HandleType>
    void normalTest()
    {
        writeSegmentGeneric(
            "d_tiny:[0, 66)|d_tiny:[44, 107)|d_tiny:[60, 160)|d_tiny:[170, 171)|merge_delta:pack_size_11");
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->getDelta()->getRows(), 0);
        ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
        constexpr size_t expected_rows = (66 - 0) + (107 - 44) + (160 - 60) + (171 - 170);
        ASSERT_EQ(seg->getStable()->getRows(), expected_rows);
        ASSERT_EQ(seg->getStable()->getDMFiles().size(), 1);
        auto dmfile = seg->getStable()->getDMFiles()[0];
        ASSERT_EQ(dmfile->getPacks(), expected_rows / 11 + static_cast<bool>(expected_rows % 11));
        const std::vector<std::vector<Int64>> expected_int_handle_packs = {
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
            {22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
            {33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43},
            {44, 44, 45, 45, 46, 46, 47, 47, 48, 48, 49, 49},
            {50, 50, 51, 51, 52, 52, 53, 53, 54, 54},
            {55, 55, 56, 56, 57, 57, 58, 58, 59, 59, 60, 60, 60},
            {61, 61, 61, 62, 62, 62, 63, 63, 63},
            {64, 64, 64, 65, 65, 65, 66, 66, 67, 67, 68, 68},
            {69, 69, 70, 70, 71, 71, 72, 72, 73, 73},
            {74, 74, 75, 75, 76, 76, 77, 77, 78, 78, 79, 79},
            {80, 80, 81, 81, 82, 82, 83, 83, 84, 84},
            {85, 85, 86, 86, 87, 87, 88, 88, 89, 89, 90, 90},
            {91, 91, 92, 92, 93, 93, 94, 94, 95, 95},
            {96, 96, 97, 97, 98, 98, 99, 99, 100, 100, 101, 101},
            {102, 102, 103, 103, 104, 104, 105, 105, 106, 106},
            {107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117},
            {118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128},
            {129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139},
            {140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150},
            {151, 152, 153, 154, 155, 156, 157, 158, 159, 170},
        };
        const auto expected_handle_packs = genHandlePacks<HandleType>(expected_int_handle_packs);
        std::unordered_map<HandleType, RowID> expected_base_versions;
        {
            RowID row_id = 0;
            for (const auto & handles : expected_handle_packs)
            {
                for (const auto & h : handles)
                {
                    if (!expected_base_versions.contains(h))
                        expected_base_versions[h] = row_id;
                    ++row_id;
                }
            }
        }

        DMFileHandleIndex<HandleType> handle_index(*dm_context, dmfile, /*start_row_id*/ 0, /*rowkey_range*/ {});
        {
            // By default, load data of all packs.
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        {
            // Load less packs.
            const auto want_read_handles = genHandles<HandleType>({0, 1, 2, 3, 69, 70, 140, 150});
            const auto expected_read_packs = {0, 9, 19};
            handle_index.calculateReadPacks(want_read_handles.begin(), want_read_handles.end());
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                if (std::find(expected_read_packs.begin(), expected_read_packs.end(), i) == expected_read_packs.end())
                {
                    ASSERT_EQ(handle_index.clipped_handle_packs[i], nullptr);
                    continue;
                }
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        {
            // Load all packs.
            const auto want_read_handles = genHandles<HandleType>({0, 12, 24, 36, 69, 74, 140, 151});
            handle_index.calculateReadPacks(want_read_handles.begin(), want_read_handles.end());
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        for (Int64 h = -10; h < 200; ++h)
        {
            HandleType handle;
            if constexpr (isCommonHandle<HandleType>())
                handle = genMockCommonHandle(h, 1);
            else
                handle = h;

            auto base_version = handle_index.getBaseVersion(*dm_context, handle);
            if (!expected_base_versions.contains(handle))
            {
                ASSERT_FALSE(base_version.has_value());
                continue;
            }
            ASSERT_TRUE(base_version.has_value());
            ASSERT_EQ(base_version.value(), expected_base_versions[handle]);
        }
    }

    template <ExtraHandleType HandleType>
    void withRangeTest()
    {
        writeSegmentGeneric(
            "d_tiny:[0, 66)|d_tiny:[44, 107)|d_tiny:[60, 160)|d_tiny:[170, 171)|merge_delta:pack_size_11");
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->getDelta()->getRows(), 0);
        ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
        constexpr size_t expected_rows = (66 - 0) + (107 - 44) + (160 - 60) + (171 - 170);
        ASSERT_EQ(seg->getStable()->getRows(), expected_rows);
        ASSERT_EQ(seg->getStable()->getDMFiles().size(), 1);
        auto dmfile = seg->getStable()->getDMFiles()[0];
        ASSERT_EQ(dmfile->getPacks(), expected_rows / 11 + static_cast<bool>(expected_rows % 11));
        const auto rowkey_range = buildRowKeyRange(47, 134, is_common_handle); // with a range that not all packs are valid
        const std::vector<std::vector<Int64>> expected_int_handle_packs = {
            {44, 44, 45, 45, 46, 46, 47, 47, 48, 48, 49, 49},
            {50, 50, 51, 51, 52, 52, 53, 53, 54, 54},
            {55, 55, 56, 56, 57, 57, 58, 58, 59, 59, 60, 60, 60},
            {61, 61, 61, 62, 62, 62, 63, 63, 63},
            {64, 64, 64, 65, 65, 65, 66, 66, 67, 67, 68, 68},
            {69, 69, 70, 70, 71, 71, 72, 72, 73, 73},
            {74, 74, 75, 75, 76, 76, 77, 77, 78, 78, 79, 79},
            {80, 80, 81, 81, 82, 82, 83, 83, 84, 84},
            {85, 85, 86, 86, 87, 87, 88, 88, 89, 89, 90, 90},
            {91, 91, 92, 92, 93, 93, 94, 94, 95, 95},
            {96, 96, 97, 97, 98, 98, 99, 99, 100, 100, 101, 101},
            {102, 102, 103, 103, 104, 104, 105, 105, 106, 106},
            {107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117},
            {118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128},
            {129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139},
        };
        const auto expected_handle_packs = genHandlePacks<HandleType>(expected_int_handle_packs);
        const auto start_row_id = 1899;
        std::unordered_map<HandleType, RowID> expected_base_versions;
        {
            using HandleRefType =
                typename std::conditional<std::is_same_v<HandleType, Int64>, Int64, std::string_view>::type;
            RowID row_id = 0;
            for (const auto & handles : expected_handle_packs)
            {
                for (const auto & h : handles)
                {
                    if (!expected_base_versions.contains(h) && inRowKeyRange(rowkey_range, HandleRefType(h)))
                        expected_base_versions[h] = row_id + start_row_id;
                    ++row_id;
                }
            }
        }

        DMFileHandleIndex<HandleType> handle_index(*dm_context, dmfile, start_row_id, rowkey_range);
        {
            // By default, load data of all packs.
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        {
            // Load less packs.
            const auto want_read_handles = genHandles<HandleType>({0, 1, 2, 3, 69, 70, 140, 150});
            const auto expected_read_packs = {5};
            handle_index.calculateReadPacks(want_read_handles.begin(), want_read_handles.end());
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                if (std::find(expected_read_packs.begin(), expected_read_packs.end(), i) == expected_read_packs.end())
                {
                    ASSERT_EQ(handle_index.clipped_handle_packs[i], nullptr);
                    continue;
                }
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        {
            // Load all packs.
            const auto want_read_handles = genHandles<HandleType>({129, 103, 87, 44, 55, 66, 77, 84});
            handle_index.calculateReadPacks(want_read_handles.begin(), want_read_handles.end());
            handle_index.loadHandleIfNotLoaded(*dm_context);
            for (size_t i = 0; i < expected_handle_packs.size(); ++i)
            {
                const auto & expected_handles = expected_handle_packs[i];
                const auto handles = ColumnView<HandleType>(*handle_index.clipped_handle_packs[i]);
                ASSERT_EQ(handles.size(), expected_handles.size());
                ASSERT_TRUE(std::equal(expected_handles.begin(), expected_handles.end(), handles.begin()));
            }
            handle_index.cleanHandleColumn();
        }

        for (Int64 h = -10; h < 200; ++h)
        {
            HandleType handle;
            if constexpr (isCommonHandle<HandleType>())
                handle = genMockCommonHandle(h, 1);
            else
                handle = h;

            auto base_version = handle_index.getBaseVersion(*dm_context, handle);
            if (!expected_base_versions.contains(handle))
            {
                ASSERT_FALSE(base_version.has_value());
                continue;
            }
            ASSERT_TRUE(base_version.has_value());
            ASSERT_EQ(base_version.value(), expected_base_versions[handle]);
        }
    }
};

INSTANTIATE_TEST_CASE_P(VersionChain, DMFileHandleIndexTest, /* is_common_handle */ ::testing::Bool());

TEST_P(DMFileHandleIndexTest, Normal)
{
    if (is_common_handle)
        normalTest<String>();
    else
        normalTest<Int64>();
}

TEST_P(DMFileHandleIndexTest, WithRange)
{
    if (is_common_handle)
        withRangeTest<String>();
    else
        withRangeTest<Int64>();
}

} // namespace DB::DM::tests

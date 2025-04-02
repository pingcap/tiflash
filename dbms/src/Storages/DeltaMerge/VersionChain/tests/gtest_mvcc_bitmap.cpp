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

#include <Storages/DeltaMerge/VersionChain/MVCCBitmapFilter.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>
#include <Storages/DeltaMerge/VersionChain/tests/mvcc_test_utils.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <gtest/gtest.h>

using namespace DB::tests;
using namespace DB::DM::tests::MVCC;

namespace DB::DM::tests
{
template <ExtraHandleType HandleType>
void randomMVCCBitmapVerify(WriteLoad write_load, UInt32 delta_rows)
try
{
    constexpr bool is_common_handle = std::is_same_v<HandleType, String>;
    auto [context, dm_context, cols, segment, segment_snapshot, random_sequences]
        = initialize(write_load, is_common_handle, delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    ASSERT_EQ(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first, 0);
    segment->placeDeltaIndex(*dm_context, segment_snapshot);
    ASSERT_EQ(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first, delta_rows);

    std::visit([&](auto & version_chain) { ASSERT_EQ(version_chain.getReplayedRows(), 0); }, segment->version_chain);
    std::ignore = std::visit(
        [&](auto & version_chain) { return version_chain.replaySnapshot(*dm_context, *segment_snapshot); },
        segment->version_chain);
    std::visit(
        [&](auto & version_chain) { ASSERT_EQ(version_chain.getReplayedRows(), delta_rows); },
        segment->version_chain);

    auto rs_results = loadPackFilterResults(*dm_context, segment_snapshot, {segment->getRowKeyRange()});
    auto bitmap_filter_delta_index = segment->buildMVCCBitmapFilter(
        *dm_context,
        segment_snapshot,
        {segment->getRowKeyRange()},
        rs_results,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        /*enable_version_chain*/ false);
    auto bitmap_filter_version_chain = segment->buildMVCCBitmapFilter(
        *dm_context,
        segment_snapshot,
        {segment->getRowKeyRange()},
        rs_results,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        /*enable_version_chain*/ true);

    if (*bitmap_filter_delta_index == *bitmap_filter_version_chain)
        return;

    ASSERT_EQ(bitmap_filter_delta_index->size(), bitmap_filter_version_chain->size());
    ASSERT_EQ(bitmap_filter_delta_index->isAllMatch(), bitmap_filter_version_chain->isAllMatch());
    for (UInt32 i = 0; i < bitmap_filter_delta_index->size(); ++i)
    {
        ASSERT_EQ(bitmap_filter_delta_index->get(i), bitmap_filter_version_chain->get(i)) << fmt::format(
            "i={}, filter1={}, filter2={}, write_load={}, delta_rows={}",
            i,
            bitmap_filter_delta_index->get(i),
            bitmap_filter_version_chain->get(i),
            magic_enum::enum_name(write_load),
            delta_rows);
    }
}
CATCH

static constexpr UInt32 max_delta_rows = 8 << 13;

TEST(RandomMVCCBitmapTest, Int64)
{
    for (auto write_load : magic_enum::enum_values<WriteLoad>())
        for (UInt32 delta_rows = 1; delta_rows <= max_delta_rows; delta_rows *= 8)
            randomMVCCBitmapVerify<Int64>(write_load, delta_rows);
}

TEST(RandomMVCCBitmapTest, String)
{
    for (auto write_load : magic_enum::enum_values<WriteLoad>())
        for (UInt32 delta_rows = 1; delta_rows <= max_delta_rows; delta_rows *= 8)
            randomMVCCBitmapVerify<String>(write_load, delta_rows);
}
} // namespace DB::DM::tests

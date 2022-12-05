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

#include <Common/CurrentMetrics.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfBitmapFilter;
} // namespace CurrentMetrics

namespace DB::DM::tests
{
std::pair<SegmentPtr, SegmentSnapshot> get
BitmapFilterPtr SegmentTestBasic::buildBitmapFilter(PageId segment_id)
{
    LOG_INFO(logger_op, "buildBitmapFilter, segment_id={}", segment_id);

    RUNTIME_CHECK(segments.find(segment_id) != segments.end());
    auto segment = segments[segment_id];
    auto snapshot = segment->createSnapshot(
        *dm_context,
        /* for_update */ false,
        CurrentMetrics::DT_SnapshotOfBitmapFilter);
    RUNTIME_CHECK(snapshot != nullptr);

    return segment->buildBitmapFilter(
        *dm_context,
        snapshot,
        {segment->getRowKeyRange()},
        nullptr,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
}

Block SegmentTestBasic::getSegmentRowId(PageId segment_id)
{
    LOG_INFO(logger_op, "getSegmentRowId, segment_id={}", segment_id);


}
}
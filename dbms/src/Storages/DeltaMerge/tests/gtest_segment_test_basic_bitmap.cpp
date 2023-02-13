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

Block mergeSegmentRowIds(std::vector<Block> && blocks)
{
    auto accumulated_block = std::move(blocks[0]);
    RUNTIME_CHECK(accumulated_block.segmentRowIdCol() != nullptr);
    for (size_t block_idx = 1; block_idx < blocks.size(); ++block_idx)
    {
        auto block = std::move(blocks[block_idx]);
        auto accu_row_id_col = accumulated_block.segmentRowIdCol();
        auto row_id_col = block.segmentRowIdCol();
        RUNTIME_CHECK(row_id_col != nullptr);
        auto mut_col = (*std::move(accu_row_id_col)).mutate();
        mut_col->insertRangeFrom(*row_id_col, 0, row_id_col->size());
        accumulated_block.setSegmentRowIdCol(std::move(mut_col));
    }
    return accumulated_block;
}

RowKeyRange SegmentTestBasic::buildRowKeyRange(Int64 begin, Int64 end)
{
    HandleRange range(begin, end);
    return RowKeyRange::fromHandleRange(range);
}

std::pair<SegmentPtr, SegmentSnapshotPtr> SegmentTestBasic::getSegmentForRead(PageIdU64 segment_id)
{
    RUNTIME_CHECK(segments.find(segment_id) != segments.end());
    auto segment = segments[segment_id];
    auto snapshot = segment->createSnapshot(
        *dm_context,
        /* for_update */ false,
        CurrentMetrics::DT_SnapshotOfBitmapFilter);
    RUNTIME_CHECK(snapshot != nullptr);
    return {segment, snapshot};
}
std::vector<Block> SegmentTestBasic::readSegment(PageIdU64 segment_id, bool need_row_id, const RowKeyRanges & ranges)
{
    auto [segment, snapshot] = getSegmentForRead(segment_id);
    ColumnDefines columns_to_read = {getExtraHandleColumnDefine(options.is_common_handle),
                                     getVersionColumnDefine()};
    auto stream = segment->getInputStreamModeNormal(
        *dm_context,
        columns_to_read,
        snapshot,
        ranges.empty() ? RowKeyRanges{segment->getRowKeyRange()} : ranges,
        nullptr,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        need_row_id);
    std::vector<Block> blks;
    for (auto blk = stream->read(); blk; blk = stream->read())
    {
        blks.push_back(blk);
    }
    return blks;
}

ColumnPtr SegmentTestBasic::getSegmentRowId(PageIdU64 segment_id, const RowKeyRanges & ranges)
{
    LOG_INFO(logger_op, "getSegmentRowId, segment_id={}", segment_id);
    auto blks = readSegment(segment_id, true, ranges);
    if (blks.empty())
    {
        return nullptr;
    }
    else
    {
        auto block = mergeSegmentRowIds(std::move(blks));
        RUNTIME_CHECK(!block.has(EXTRA_HANDLE_COLUMN_NAME));
        RUNTIME_CHECK(block.segmentRowIdCol() != nullptr);
        return block.segmentRowIdCol();
    }
}

ColumnPtr SegmentTestBasic::getSegmentHandle(PageIdU64 segment_id, const RowKeyRanges & ranges)
{
    LOG_INFO(logger_op, "getSegmentHandle, segment_id={}", segment_id);
    auto blks = readSegment(segment_id, false, ranges);
    if (blks.empty())
    {
        return nullptr;
    }
    else
    {
        auto block = vstackBlocks(std::move(blks));
        RUNTIME_CHECK(block.has(EXTRA_HANDLE_COLUMN_NAME));
        RUNTIME_CHECK(block.segmentRowIdCol() == nullptr);
        return block.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    }
}

void SegmentTestBasic::writeSegmentWithDeleteRange(PageIdU64 segment_id, Int64 begin, Int64 end)
{
    auto range = buildRowKeyRange(begin, end);
    RUNTIME_CHECK(segments.find(segment_id) != segments.end());
    auto segment = segments[segment_id];
    RUNTIME_CHECK(segment->write(*dm_context, range));
}
} // namespace DB::DM::tests

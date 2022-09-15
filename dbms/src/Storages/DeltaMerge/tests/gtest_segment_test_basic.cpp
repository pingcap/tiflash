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

namespace DB
{
namespace DM
{
extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context,
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path,
                                    DMFileBlockOutputStream::Flags flags);

namespace tests
{
void SegmentTestBasic::reloadWithOptions(SegmentTestOptions config)
{
    {
        auto const seed = std::random_device{}();
        random = std::mt19937{seed};
    }

    logger = Logger::get("SegmentTest");
    logger_op = Logger::get("SegmentTestOperation");

    TiFlashStorageTestBasic::SetUp();
    options = config;
    table_columns = std::make_shared<ColumnDefines>();

    root_segment = reload(config.is_common_handle, nullptr, std::move(config.db_settings));
    ASSERT_EQ(root_segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    segments.clear();
    segments[DELTA_MERGE_FIRST_SEGMENT_ID] = root_segment;
}

PageId SegmentTestBasic::createNewSegmentWithSomeData()
{
    SegmentPtr new_segment;
    std::tie(root_segment, new_segment) = root_segment->split(dmContext(), tableColumns());

    const size_t num_rows_write_per_batch = 100;
    {
        // write to segment and flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write_per_batch, false);
        new_segment->write(dmContext(), std::move(block), true);
    }
    {
        // write to segment and don't flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write_per_batch, 2 * num_rows_write_per_batch, false);
        new_segment->write(dmContext(), std::move(block), false);
    }
    return new_segment->segmentId();
}

size_t SegmentTestBasic::getSegmentRowNumWithoutMVCC(PageId segment_id)
{
    auto segment = segments[segment_id];
    auto in = segment->getInputStreamRaw(dmContext(), *tableColumns());
    return getInputStreamNRows(in);
}

size_t SegmentTestBasic::getSegmentRowNum(PageId segment_id)
{
    auto segment = segments[segment_id];
    auto in = segment->getInputStream(dmContext(), *tableColumns(), {segment->getRowKeyRange()});
    return getInputStreamNRows(in);
}

std::optional<PageId> SegmentTestBasic::splitSegment(PageId segment_id, bool check_rows)
{
    LOG_FMT_INFO(logger_op, "splitSegment, segment_id={}", segment_id);

    auto origin_segment = segments[segment_id];
    size_t origin_segment_row_num = getSegmentRowNum(segment_id);

    LOG_FMT_DEBUG(logger, "begin split, segment_id={} rows={}", segment_id, origin_segment_row_num);

    auto [left, right] = origin_segment->split(dmContext(), tableColumns());
    if (!left && !right)
    {
        LOG_FMT_DEBUG(logger, "split not succeeded, segment_id={} rows={}", segment_id, origin_segment_row_num);
        return std::nullopt;
    }

    RUNTIME_CHECK(left && right);
    RUNTIME_CHECK(left->segmentId() == segment_id, segment_id, left->info());
    segments[left->segmentId()] = left; // The left segment is updated
    segments[right->segmentId()] = right;

    auto left_rows = getSegmentRowNum(segment_id);
    auto right_rows = getSegmentRowNum(right->segmentId());

    if (check_rows)
        EXPECT_EQ(origin_segment_row_num, left_rows + right_rows);

    LOG_FMT_DEBUG(logger, "split finish, left_id={} left_rows={} right_id={} right_rows={}", left->segmentId(), left_rows, right->segmentId(), right_rows);

    return right->segmentId();
}

void SegmentTestBasic::mergeSegment(const std::vector<PageId> & segments_id, bool check_rows)
{
    LOG_FMT_INFO(logger_op, "mergeSegment, segments=[{}]", fmt::join(segments_id, ","));

    RUNTIME_CHECK(segments_id.size() >= 2, segments_id.size());

    std::vector<SegmentPtr> segments_to_merge;
    std::vector<size_t> segments_rows;
    size_t merged_rows = 0;
    segments_to_merge.reserve(segments_id.size());
    segments_rows.reserve(segments_id.size());

    for (const auto segment_id : segments_id)
    {
        auto it = segments.find(segment_id);
        RUNTIME_CHECK(it != segments.end(), segment_id);
        segments_to_merge.emplace_back(it->second);

        auto rows = getSegmentRowNum(segment_id);
        segments_rows.emplace_back(rows);
        merged_rows += rows;
    }

    LOG_FMT_DEBUG(logger, "begin merge, segments=[{}] each_rows=[{}]", fmt::join(segments_id, ","), fmt::join(segments_rows, ","));

    SegmentPtr merged_segment = Segment::merge(dmContext(), tableColumns(), segments_to_merge);
    if (!merged_segment)
    {
        LOG_FMT_DEBUG(logger, "merge not succeeded, segments=[{}] each_rows=[{}]", fmt::join(segments_id, ","), fmt::join(segments_rows, ","));
        return;
    }

    for (const auto segment_id : segments_id)
        segments.erase(segments.find(segment_id));
    segments[merged_segment->segmentId()] = merged_segment;

    if (check_rows)
        EXPECT_EQ(getSegmentRowNum(merged_segment->segmentId()), merged_rows);

    LOG_FMT_DEBUG(logger, "merge finish, merged_segment_id={} merge_from_segments=[{}] merged_rows={}", merged_segment->segmentId(), fmt::join(segments_id, ","), merged_rows);
}

void SegmentTestBasic::mergeSegmentDelta(PageId segment_id, bool check_rows)
{
    LOG_FMT_INFO(logger_op, "mergeSegmentDelta, segment_id={}", segment_id);

    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNum(segment_id);
    SegmentPtr merged_segment = segment->mergeDelta(dmContext(), tableColumns());
    segments[merged_segment->segmentId()] = merged_segment;
    if (check_rows)
    {
        EXPECT_EQ(getSegmentRowNum(merged_segment->segmentId()), segment_row_num);
    }
}

void SegmentTestBasic::flushSegmentCache(PageId segment_id)
{
    LOG_FMT_INFO(logger_op, "flushSegmentCache, segment_id={}", segment_id);

    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNum(segment_id);
    segment->flushCache(dmContext());
    EXPECT_EQ(getSegmentRowNum(segment_id), segment_row_num);
}

std::pair<Int64, Int64> SegmentTestBasic::getSegmentKeyRange(SegmentPtr segment)
{
    Int64 start_key, end_key;
    if (!options.is_common_handle)
    {
        start_key = segment->getRowKeyRange().getStart().int_value;
        end_key = segment->getRowKeyRange().getEnd().int_value;
        return {start_key, end_key};
    }

    const auto & range = segment->getRowKeyRange();
    if (range.isStartInfinite())
    {
        start_key = std::numeric_limits<Int64>::min();
    }
    else
    {
        EXPECT_EQ(range.getStart().data[0], TiDB::CodecFlagInt);
        size_t cursor = 1;
        start_key = DecodeInt64(cursor, String(range.getStart().data, range.getStart().size));
    }
    if (range.isEndInfinite())
    {
        end_key = std::numeric_limits<Int64>::max();
    }
    else
    {
        EXPECT_EQ(range.getEnd().data[0], TiDB::CodecFlagInt);
        size_t cursor = 1;
        end_key = DecodeInt64(cursor, String(range.getEnd().data, range.getEnd().size));
    }
    return {start_key, end_key};
}

std::vector<Block> SegmentTestBasic::prepareWriteBlocksInSegmentRange(PageId segment_id, UInt64 total_write_rows, std::optional<Int64> write_start_key, bool is_deleted)
{
    auto segment = segments[segment_id];
    auto [segment_start_key, segment_end_key] = getSegmentKeyRange(segment);
    auto segment_max_rows = static_cast<UInt64>(segment_end_key - segment_start_key);

    if (segment_max_rows == 0)
        return {};

    if (write_start_key.has_value())
    {
        // When write start key is specified, the caller must know exactly the segment range.
        RUNTIME_CHECK(*write_start_key >= segment_start_key);
        RUNTIME_CHECK(static_cast<UInt64>(segment_end_key - *write_start_key) > 0);
    }

    if (!write_start_key.has_value())
    {
        // When write start key is unspecified, we will:
        // A. If the segment is large enough, we randomly pick a write start key in the range.
        // B. If the segment is small, we write from the beginning.
        if (segment_max_rows > total_write_rows)
        {
            write_start_key = std::uniform_int_distribution<Int64>{segment_start_key, segment_end_key - static_cast<Int64>(total_write_rows)}(random);
        }
        else
        {
            write_start_key = segment_start_key;
        }
    }

    UInt64 max_write_rows_each_round = static_cast<UInt64>(segment_end_key - *write_start_key);
    RUNTIME_CHECK(max_write_rows_each_round > 0);
    RUNTIME_CHECK(*write_start_key >= segment_start_key);

    std::vector<Block> blocks;

    // If the length of segment key range is larger than `write_rows`, then
    // write the new data with the same tso in one block.
    // Otherwise create multiple block with increasing tso until the `remain_row_num`
    // down to 0.
    UInt64 remaining_rows = total_write_rows;
    while (remaining_rows > 0)
    {
        UInt64 write_rows_this_round = std::min(remaining_rows, max_write_rows_each_round);
        RUNTIME_CHECK(write_rows_this_round > 0);
        Int64 write_end_key_this_round = *write_start_key + static_cast<Int64>(write_rows_this_round);
        RUNTIME_CHECK(write_end_key_this_round <= segment_end_key);

        Block block = DMTestEnv::prepareSimpleWriteBlock(
            *write_start_key, //
            write_end_key_this_round,
            false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle, 1, true, is_deleted);

        blocks.emplace_back(block);

        remaining_rows -= write_rows_this_round;
        version++;

        LOG_FMT_DEBUG(logger, "Prepared block for write, block_range=[{}, {}) (rows={}), total_rows_to_write={} remain_rows={}", //
                      *write_start_key, write_end_key_this_round, write_rows_this_round, total_write_rows, remaining_rows);
    }

    return blocks;
}

void SegmentTestBasic::writeSegment(PageId segment_id, UInt64 write_rows)
{
    LOG_FMT_INFO(logger_op, "writeSegment, segment_id={} rows={}", segment_id, write_rows);

    if (write_rows == 0)
        return;

    RUNTIME_CHECK(write_rows > 0);
    RUNTIME_CHECK(write_rows < std::numeric_limits<Int64>::max());

    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNumWithoutMVCC(segment_id);
    auto [start_key, end_key] = getSegmentKeyRange(segment);

    LOG_FMT_DEBUG(logger, "write to segment, segment={} segment_rows={} start_key={} end_key={}", segment->info(), segment_row_num, start_key, end_key);

    auto segment_max_rows = static_cast<UInt64>(end_key - start_key);
    if (segment_max_rows == 0)
        return;
    // If the length of segment key range is larger than `write_rows`, then
    // write the new data with the same tso in one block.
    // Otherwise create multiple block with increasing tso until the `remain_row_num`
    // down to 0.
    UInt64 remain_row_num = 0;
    if (segment_max_rows > write_rows)
    {
        // The segment range is large enough, let's randomly pick a start key:
        //    Suppose we have segment range = [0, 11), which could contain at most 11 rows.
        //    Now we want to write 10 rows -- The write start key could be randomized in [0, 1].
        start_key = std::uniform_int_distribution<Int64>{start_key, end_key - static_cast<Int64>(write_rows)}(random);
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - segment_max_rows;
    }
    {
        // write to segment and not flush
        LOG_FMT_DEBUG(logger, "write block to segment, block_range=[{}, {})", start_key, end_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        segment->write(dmContext(), std::move(block), false);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, segment_max_rows);
        LOG_FMT_DEBUG(logger, "write block to segment, block_range=[{}, {})", start_key, write_num + start_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        segment->write(dmContext(), std::move(block), false);
        remain_row_num -= write_num;
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::ingestDTFileIntoSegment(PageId segment_id, UInt64 write_rows)
{
    LOG_FMT_INFO(logger_op, "ingestDTFileIntoSegment, segment_id={} rows={}", segment_id, write_rows);

    if (write_rows == 0)
        return;

    auto write_data = [&](SegmentPtr segment, const Block & block) {
        WriteBatches ingest_wbs(dm_context->storage_pool, dm_context->getWriteLimiter());
        auto delegator = storage_path_pool->getStableDiskDelegator();
        auto parent_path = delegator.choosePath();
        auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        DMFileBlockOutputStream::Flags flags;
        auto dm_file = writeIntoNewDMFile(
            *dm_context,
            table_columns,
            input_stream,
            file_id,
            parent_path,
            flags);
        ingest_wbs.data.putExternal(file_id, /* tag */ 0);
        ingest_wbs.writeLogAndData();
        delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), parent_path);
        {
            WriteBatches wbs(dm_context->storage_pool, dm_context->getWriteLimiter());
            auto ref_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            wbs.data.putRefPage(ref_id, dm_file->pageId());
            auto ref_file = DMFile::restore(dm_context->db_context.getFileProvider(), file_id, ref_id, parent_path, DMFile::ReadMetaMode::all());
            wbs.writeLogAndData();
            auto column_file = std::make_shared<ColumnFileBig>(*dm_context, ref_file, segment->getRowKeyRange());
            ColumnFiles column_files;
            column_files.push_back(column_file);
            ASSERT_TRUE(segment->ingestColumnFiles(*dm_context, segment->getRowKeyRange(), column_files, /* clear_data_in_range */ true));
        }
        ingest_wbs.rollbackWrittenLogAndData();
    };

    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNumWithoutMVCC(segment_id);
    auto [start_key, end_key] = getSegmentKeyRange(segment);

    auto segment_max_rows = static_cast<UInt64>(end_key - start_key);
    if (segment_max_rows == 0)
        return;
    // If the length of segment key range is larger than `write_rows`, then
    // write the new data with the same tso in one block.
    // Otherwise create multiple block with increasing tso until the `remain_row_num`
    // down to 0.
    UInt64 remain_row_num = 0;
    if (segment_max_rows > write_rows)
    {
        start_key = std::uniform_int_distribution<Int64>{start_key, end_key - static_cast<Int64>(write_rows)}(random);
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - segment_max_rows;
    }
    {
        // write to segment and not flush
        LOG_FMT_DEBUG(logger, "ingest block to segment, block_range=[{}, {})", start_key, end_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        write_data(segment, block);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, segment_max_rows);
        LOG_FMT_DEBUG(logger, "ingest block to segment, block_range=[{}, {})", start_key, write_num + start_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        write_data(segment, block);
        remain_row_num -= write_num;
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::writeSegmentWithDeletedPack(PageId segment_id, UInt64 write_rows)
{
    LOG_FMT_INFO(logger_op, "writeSegmentWithDeletedPack, segment_id={}", segment_id);

    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNumWithoutMVCC(segment_id);
    auto [start_key, end_key] = getSegmentKeyRange(segment);

    auto segment_max_rows = static_cast<UInt64>(end_key - start_key);
    if (segment_max_rows == 0)
        return;
    // If the length of segment key range is larger than `write_rows`, then
    // write the new data with the same tso in one block.
    // Otherwise create multiple block with increasing tso until the `remain_row_num`
    // down to 0.
    UInt64 remain_row_num = 0;
    if (segment_max_rows > write_rows)
    {
        start_key = std::uniform_int_distribution<Int64>{start_key, end_key - static_cast<Int64>(write_rows)}(random);
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - segment_max_rows;
    }
    {
        // write to segment and not flush
        LOG_FMT_DEBUG(logger, "write block to segment, block_range=[{}, {})", start_key, end_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle, 1, true, true);
        segment->write(dmContext(), std::move(block), true);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, segment_max_rows);
        LOG_FMT_DEBUG(logger, "write block to segment, block_range=[{}, {})", start_key, write_num + start_key);
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle, 1, true, true);
        segment->write(dmContext(), std::move(block), true);
        remain_row_num -= write_num;
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::deleteRangeSegment(PageId segment_id)
{
    LOG_FMT_INFO(logger_op, "deleteRangeSegment, segment_id={}", segment_id);

    auto segment = segments[segment_id];
    segment->write(dmContext(), /*delete_range*/ segment->getRowKeyRange());
    EXPECT_EQ(getSegmentRowNum(segment_id), 0);
}

void SegmentTestBasic::replaceDataSegment(PageId segment_id, const DMFilePtr & file)
{
    auto segment = segments[segment_id];
    auto new_segment = segment->dangerouslyReplaceDataForTest(dmContext(), file);
    segments[new_segment->segmentId()] = new_segment;
    EXPECT_EQ(file->getRows(), getSegmentRowNumWithoutMVCC(segment_id));
}

void SegmentTestBasic::replaceDataSegment(PageId segment_id, const Block & block)
{
    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto parent_path = delegator.choosePath();
    auto file_provider = db_context->getFileProvider();

    auto current_seg = segments[DELTA_MERGE_FIRST_SEGMENT_ID];

    WriteBatches ingest_wbs(dm_context->storage_pool, dm_context->getWriteLimiter());

    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto dm_file = writeIntoNewDMFile(
        *dm_context,
        table_columns,
        input_stream,
        file_id,
        parent_path,
        {});

    ingest_wbs.data.putExternal(file_id, /* tag */ 0);
    ingest_wbs.writeLogAndData();
    delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), parent_path);

    replaceDataSegment(segment_id, dm_file);

    dm_file->enableGC();
    ingest_wbs.rollbackWrittenLogAndData();

    EXPECT_EQ(block.rows(), getSegmentRowNumWithoutMVCC(segment_id));
}

void SegmentTestBasic::writeRandomSegment()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
    LOG_FMT_DEBUG(logger, "start random write, segment_id={} write_rows={} all_segments={}", random_segment_id, write_rows, segments.size());
    writeSegment(random_segment_id, write_rows);
}

void SegmentTestBasic::writeRandomSegmentWithDeletedPack()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
    LOG_FMT_DEBUG(logger, "start random write delete, segment_id={} write_rows={} all_segments={}", random_segment_id, write_rows, segments.size());
    writeSegmentWithDeletedPack(random_segment_id, write_rows);
}

void SegmentTestBasic::deleteRangeRandomSegment()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_DEBUG(logger, "start random delete range, segment_id={} all_segments={}", random_segment_id, segments.size());
    deleteRangeSegment(random_segment_id);
}

void SegmentTestBasic::splitRandomSegment()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_DEBUG(logger, "start random split, segment_id={} all_segments={}", random_segment_id, segments.size());
    splitSegment(random_segment_id);
}

void SegmentTestBasic::mergeRandomSegment()
{
    if (segments.size() < 2)
        return;
    auto segments_id = getRandomMergeableSegments();
    LOG_FMT_DEBUG(logger, "start random merge, segments_id=[{}] all_segments={}", fmt::join(segments_id, ","), segments.size());
    mergeSegment(segments_id);
}

void SegmentTestBasic::mergeDeltaRandomSegment()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_DEBUG(logger, "start random merge delta, segment_id={} all_segments={}", random_segment_id, segments.size());
    mergeSegmentDelta(random_segment_id);
}

void SegmentTestBasic::flushCacheRandomSegment()
{
    if (segments.empty())
        return;
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_DEBUG(logger, "start random flush cache, segment_id={} all_segments={}", random_segment_id, segments.size());
    flushSegmentCache(random_segment_id);
}

void SegmentTestBasic::randomSegmentTest(size_t operator_count)
{
    auto probabilities = std::vector<double>{};
    std::transform(segment_operator_entries.begin(), segment_operator_entries.end(), std::back_inserter(probabilities), [](auto v) { return v.first; });

    auto dist = std::discrete_distribution<size_t>{probabilities.begin(), probabilities.end()};
    for (size_t i = 0; i < operator_count; i++)
    {
        auto op_idx = dist(random);
        segment_operator_entries[op_idx].second();
    }
}

PageId SegmentTestBasic::getRandomSegmentId() // Complexity is O(n)
{
    RUNTIME_CHECK(!segments.empty());
    auto dist = std::uniform_int_distribution<size_t>{0, segments.size() - 1};
    auto pick_n = dist(random);
    auto it = segments.begin();
    std::advance(it, pick_n);
    auto segment_id = it->second->segmentId();
    RUNTIME_CHECK(segments.find(segment_id) != segments.end(), segment_id);
    RUNTIME_CHECK(segments[segment_id]->segmentId() == segment_id);
    return segment_id;
}

std::vector<PageId> SegmentTestBasic::getRandomMergeableSegments()
{
    RUNTIME_CHECK(segments.size() >= 2, segments.size());

    // Merge 2~6 segments (at most 1/2 of all segments).
    auto max_merge_segments = std::uniform_int_distribution<int>{2, std::clamp(static_cast<int>(segments.size()) / 2, 2, 6)}(random);

    std::vector<PageId> segments_id;
    segments_id.reserve(max_merge_segments);

    while (true)
    {
        segments_id.clear();
        segments_id.push_back(getRandomSegmentId());

        for (int i = 1; i < max_merge_segments; i++)
        {
            auto last_segment_id = segments_id.back();
            RUNTIME_CHECK(segments.find(last_segment_id) != segments.end(), last_segment_id);
            auto last_segment = segments[last_segment_id];
            if (last_segment->getRowKeyRange().isEndInfinite())
                break;

            auto next_segment_id = last_segment->nextSegmentId();
            RUNTIME_CHECK(segments.find(next_segment_id) != segments.end(), last_segment->info());
            auto next_segment = segments[next_segment_id];
            RUNTIME_CHECK(next_segment->segmentId() == next_segment_id, next_segment->info(), next_segment_id);
            RUNTIME_CHECK(compare(last_segment->getRowKeyRange().getEnd(), next_segment->getRowKeyRange().getStart()) == 0, last_segment->info(), next_segment->info());
            segments_id.push_back(next_segment_id);
        }

        if (segments_id.size() >= 2)
            break;
    }

    return segments_id;
}

SegmentPtr SegmentTestBasic::reload(bool is_common_handle, const ColumnDefinesPtr & pre_define_columns, DB::Settings && db_settings)
{
    TiFlashStorageTestBasic::reload(std::move(db_settings));
    storage_path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
    storage_pool = std::make_unique<StoragePool>(*db_context, NAMESPACE_ID, *storage_path_pool, "test.t1");
    storage_pool->restore();
    ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID) : pre_define_columns;
    setColumns(cols);

    return Segment::newSegment(*dm_context, table_columns, RowKeyRange::newAll(is_common_handle, 1), storage_pool->newMetaPageId(), 0);
}

void SegmentTestBasic::setColumns(const ColumnDefinesPtr & columns)
{
    *table_columns = *columns;

    dm_context = std::make_unique<DMContext>(*db_context,
                                             *storage_path_pool,
                                             *storage_pool,
                                             /*min_version_*/ 0,
                                             settings.not_compress_columns,
                                             options.is_common_handle,
                                             1,
                                             db_context->getSettingsRef());
}
} // namespace tests
} // namespace DM
} // namespace DB

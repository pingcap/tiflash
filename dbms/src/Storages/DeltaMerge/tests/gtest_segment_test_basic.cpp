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

    size_t num_rows_read = 0;
    in->readPrefix();
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
    }
    in->readSuffix();
    return num_rows_read;
}

size_t SegmentTestBasic::getSegmentRowNum(PageId segment_id)
{
    auto segment = segments[segment_id];
    auto in = segment->getInputStream(dmContext(), *tableColumns(), {segment->getRowKeyRange()});

    size_t num_rows_read = 0;
    in->readPrefix();
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
    }
    in->readSuffix();
    return num_rows_read;
}

void SegmentTestBasic::checkSegmentRow(PageId segment_id, size_t expected_row_num)
{
    auto segment = segments[segment_id];
    // read written data
    auto in = segment->getInputStream(dmContext(), *tableColumns(), {segment->getRowKeyRange()});

    size_t num_rows_read = 0;
    in->readPrefix();
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
    }
    in->readSuffix();
    ASSERT_EQ(num_rows_read, expected_row_num);
}

std::optional<PageId> SegmentTestBasic::splitSegment(PageId segment_id, bool check_rows)
{
    auto origin_segment = segments[segment_id];
    size_t origin_segment_row_num = getSegmentRowNum(segment_id);
    SegmentPtr segment, new_segment;
    std::tie(segment, new_segment) = origin_segment->split(dmContext(), tableColumns());
    if (new_segment)
    {
        segments[new_segment->segmentId()] = new_segment;
        segments[segment_id] = segment;

        if (check_rows)
        {
            EXPECT_EQ(origin_segment_row_num, getSegmentRowNum(segment_id) + getSegmentRowNum(new_segment->segmentId()));
        }
        return new_segment->segmentId();
    }
    return std::nullopt;
}

void SegmentTestBasic::mergeSegment(PageId left_segment_id, PageId right_segment_id, bool check_rows)
{
    auto left_segment = segments[left_segment_id];
    auto right_segment = segments[right_segment_id];

    size_t left_segment_row_num = getSegmentRowNum(left_segment_id);
    size_t right_segment_row_num = getSegmentRowNum(right_segment_id);
    LOG_FMT_TRACE(&Poco::Logger::root(), "merge in segment:{}:{} and {}:{}", left_segment->segmentId(), left_segment_row_num, right_segment->segmentId(), right_segment_row_num);

    SegmentPtr merged_segment = Segment::merge(dmContext(), tableColumns(), left_segment, right_segment);
    segments[merged_segment->segmentId()] = merged_segment;
    auto it = segments.find(right_segment->segmentId());
    if (it != segments.end())
    {
        segments.erase(it);
    }
    if (check_rows)
    {
        EXPECT_EQ(getSegmentRowNum(merged_segment->segmentId()), left_segment_row_num + right_segment_row_num);
    }
}

void SegmentTestBasic::mergeSegmentDelta(PageId segment_id, bool check_rows)
{
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
    EXPECT_EQ(segment->getRowKeyRange().getStart().data[0], TiDB::CodecFlagInt);
    EXPECT_EQ(segment->getRowKeyRange().getEnd().data[0], TiDB::CodecFlagInt);
    {
        size_t cursor = 1;
        start_key = DecodeInt64(cursor, String(segment->getRowKeyRange().getStart().data, segment->getRowKeyRange().getStart().size));
    }
    {
        size_t cursor = 1;
        end_key = DecodeInt64(cursor, String(segment->getRowKeyRange().getEnd().data, segment->getRowKeyRange().getEnd().size));
    }
    return {start_key, end_key};
}

void SegmentTestBasic::writeSegment(PageId segment_id, UInt64 write_rows)
{
    if (write_rows == 0)
    {
        return;
    }
    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNumWithoutMVCC(segment_id);
    std::pair<Int64, Int64> keys = getSegmentKeyRange(segment);
    Int64 start_key = keys.first;
    Int64 end_key = keys.second;
    UInt64 remain_row_num = 0;
    if (static_cast<UInt64>(end_key - start_key) > write_rows)
    {
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - static_cast<UInt64>(end_key - start_key);
    }
    {
        // write to segment and not flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        segment->write(dmContext(), std::move(block), false);
        LOG_FMT_TRACE(&Poco::Logger::root(), "write key range [{}, {})", start_key, end_key);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, static_cast<UInt64>(end_key - start_key));
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        segment->write(dmContext(), std::move(block), false);
        remain_row_num -= write_num;
        LOG_FMT_TRACE(&Poco::Logger::root(), "write key range [{}, {})", start_key, write_num + start_key);
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::ingestDTFileIntoSegment(PageId segment_id, UInt64 write_rows)
{
    if (write_rows == 0)
    {
        return;
    }

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
    std::pair<Int64, Int64> keys = getSegmentKeyRange(segment);
    Int64 start_key = keys.first;
    Int64 end_key = keys.second;
    UInt64 remain_row_num = 0;
    if (static_cast<UInt64>(end_key - start_key) > write_rows)
    {
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - static_cast<UInt64>(end_key - start_key);
    }
    {
        // write to segment and not flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        write_data(segment, block);
        LOG_FMT_TRACE(&Poco::Logger::root(), "ingest key range [{}, {})", start_key, end_key);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, static_cast<UInt64>(end_key - start_key));
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle);
        write_data(segment, block);
        remain_row_num -= write_num;
        LOG_FMT_TRACE(&Poco::Logger::root(), "ingest key range [{}, {})", start_key, write_num + start_key);
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::writeSegmentWithDeletedPack(PageId segment_id)
{
    UInt64 write_rows = DEFAULT_MERGE_BLOCK_SIZE;
    auto segment = segments[segment_id];
    size_t segment_row_num = getSegmentRowNumWithoutMVCC(segment_id);
    std::pair<Int64, Int64> keys = getSegmentKeyRange(segment);
    Int64 start_key = keys.first;
    Int64 end_key = keys.second;
    UInt64 remain_row_num = 0;
    if (static_cast<UInt64>(end_key - start_key) > write_rows)
    {
        end_key = start_key + write_rows;
    }
    else
    {
        remain_row_num = write_rows - static_cast<UInt64>(end_key - start_key);
    }
    {
        // write to segment and not flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, end_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle, 1, true, true);
        segment->write(dmContext(), std::move(block), true);
        LOG_FMT_TRACE(&Poco::Logger::root(), "write key range [{}, {})", start_key, end_key);
        version++;
    }
    while (remain_row_num > 0)
    {
        UInt64 write_num = std::min(remain_row_num, static_cast<UInt64>(end_key - start_key));
        Block block = DMTestEnv::prepareSimpleWriteBlock(start_key, write_num + start_key, false, version, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, options.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, options.is_common_handle, 1, true, true);
        segment->write(dmContext(), std::move(block), true);
        remain_row_num -= write_num;
        LOG_FMT_TRACE(&Poco::Logger::root(), "write key range [{}, {})", start_key, write_num + start_key);
        version++;
    }
    EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), segment_row_num + write_rows);
}

void SegmentTestBasic::deleteRangeSegment(PageId segment_id)
{
    auto segment = segments[segment_id];
    segment->write(dmContext(), /*delete_range*/ segment->getRowKeyRange());
    EXPECT_EQ(getSegmentRowNum(segment_id), 0);
}

void SegmentTestBasic::writeRandomSegment()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start write segment:{}", random_segment_id);
    writeSegment(random_segment_id);
}
void SegmentTestBasic::writeRandomSegmentWithDeletedPack()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start write segment with deleted pack:{}", random_segment_id);
    writeSegmentWithDeletedPack(random_segment_id);
}

void SegmentTestBasic::deleteRangeRandomSegment()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start delete range segment:{}", random_segment_id);
    deleteRangeSegment(random_segment_id);
}

void SegmentTestBasic::splitRandomSegment()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start split segment:{}", random_segment_id);
    splitSegment(random_segment_id);
}

void SegmentTestBasic::mergeRandomSegment()
{
    if (segments.empty() || segments.size() == 1)
    {
        return;
    }
    std::pair<PageId, PageId> segment_pair;
    segment_pair = getRandomMergeablePair();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start merge segment:{} and {}", segment_pair.first, segment_pair.second);
    mergeSegment(segment_pair.first, segment_pair.second);
}

void SegmentTestBasic::mergeDeltaRandomSegment()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start merge delta in segment:{}", random_segment_id);
    mergeSegmentDelta(random_segment_id);
}

void SegmentTestBasic::flushCacheRandomSegment()
{
    if (segments.empty())
    {
        return;
    }
    PageId random_segment_id = getRandomSegmentId();
    LOG_FMT_TRACE(&Poco::Logger::root(), "start flush cache in segment:{}", random_segment_id);
    flushSegmentCache(random_segment_id);
}

void SegmentTestBasic::randomSegmentTest(size_t operator_count)
{
    for (size_t i = 0; i < operator_count; i++)
    {
        auto op = static_cast<SegmentOperaterType>(random() % SegmentOperaterMax);
        segment_operator_entries[op]();
    }
}

PageId SegmentTestBasic::getRandomSegmentId()
{
    auto max_segment_id = segments.rbegin()->first;
    PageId random_segment_id = random() % (max_segment_id + 1);
    auto it = segments.find(random_segment_id);
    while (it == segments.end())
    {
        random_segment_id = random() % (max_segment_id + 1);
        it = segments.find(random_segment_id);
    }
    return random_segment_id;
}

std::pair<PageId, PageId> SegmentTestBasic::getRandomMergeablePair()
{
    while (true)
    {
        PageId random_left_segment_id = getRandomSegmentId();
        PageId random_right_segment_id = random_left_segment_id;
        while (random_right_segment_id == random_left_segment_id)
        {
            random_right_segment_id = getRandomSegmentId();
        }
        auto left_segment = segments[random_left_segment_id];
        auto right_segment = segments[random_right_segment_id];
        if (compare(left_segment->getRowKeyRange().getEnd(), right_segment->getRowKeyRange().getStart()) != 0 || left_segment->nextSegmentId() != right_segment->segmentId())
        {
            continue;
        }
        return {random_left_segment_id, random_right_segment_id};
    }
}

RowKeyRange SegmentTestBasic::commonHandleKeyRange()
{
    String start_key, end_key;
    {
        WriteBufferFromOwnString ss;
        ::DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        ::DB::EncodeInt64(std::numeric_limits<Int64>::min(), ss);
        start_key = ss.releaseStr();
    }
    {
        WriteBufferFromOwnString ss;
        ::DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        ::DB::EncodeInt64(std::numeric_limits<Int64>::max(), ss);
        end_key = ss.releaseStr();
    }
    return RowKeyRange(RowKeyValue(true, std::make_shared<String>(start_key), 0), RowKeyValue(true, std::make_shared<String>(end_key), 0), true, 1);
}

SegmentPtr SegmentTestBasic::reload(bool is_common_handle, const ColumnDefinesPtr & pre_define_columns, DB::Settings && db_settings)
{
    TiFlashStorageTestBasic::reload(std::move(db_settings));
    storage_path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
    storage_pool = std::make_unique<StoragePool>(*db_context, /*ns_id*/ 100, *storage_path_pool, "test.t1");
    storage_pool->restore();
    ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID) : pre_define_columns;
    setColumns(cols);

    return Segment::newSegment(*dm_context, table_columns, is_common_handle ? commonHandleKeyRange() : RowKeyRange::newAll(is_common_handle, 1), storage_pool->newMetaPageId(), 0);
}

void SegmentTestBasic::setColumns(const ColumnDefinesPtr & columns)
{
    *table_columns = *columns;

    dm_context = std::make_unique<DMContext>(*db_context,
                                             *storage_path_pool,
                                             *storage_pool,
                                             0,
                                             /*min_version_*/ 0,
                                             settings.not_compress_columns,
                                             options.is_common_handle,
                                             1,
                                             db_context->getSettingsRef());
}
} // namespace tests
} // namespace DM
} // namespace DB

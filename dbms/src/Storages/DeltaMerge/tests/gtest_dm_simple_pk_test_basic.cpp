// Copyright 2023 PingCAP, Inc.
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

#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>
#include <TestUtils/InputStreamTestUtils.h>

namespace DB
{
namespace FailPoints
{
extern const char skip_check_segment_update[];
} // namespace FailPoints

namespace DM
{

extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);

namespace tests
{
void SimplePKTestBasic::reload()
{
    TiFlashStorageTestBasic::SetUp();

    version = 0;

    auto cols = DMTestEnv::getDefaultColumns(
        is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
    store = DeltaMergeStore::create(
        *db_context,
        false,
        "test",
        DB::base::TiFlashStorageTestBasic::getCurrentFullTestName(),
        NullspaceID,
        101,
        /*pk_col_id*/ 0,
        true,
        *cols,
        (*cols)[0],
        is_common_handle,
        1,
        nullptr,
        DeltaMergeStore::Settings());
    dm_context = store->newDMContext(
        *db_context,
        db_context->getSettingsRef(),
        DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
    db_context->dropMinMaxIndexCache();
}

SegmentPtr SimplePKTestBasic::getSegmentAt(Int64 key) const
{
    auto row_key = buildRowKey(key);
    auto [segment, is_empty] = store->getSegmentByStartKey(row_key.toRowKeyValueRef(), true, true);
    std::shared_lock lock(store->read_write_mutex);
    RUNTIME_CHECK(store->isSegmentValid(lock, segment));
    return segment;
}

void SimplePKTestBasic::ensureSegmentBreakpoints(const std::vector<Int64> & breakpoints, bool use_logical_split)
{
    LOG_INFO(
        logger_op,
        "ensureSegmentBreakpoints [{}] logical_split={}",
        fmt::join(breakpoints, ","),
        use_logical_split);

    for (const auto & bp : breakpoints)
    {
        auto bp_key = buildRowKey(bp);

        while (true)
        {
            auto [segment, is_empty] = store->getSegmentByStartKey(bp_key.toRowKeyValueRef(), true, true);
            // The segment is already break at the boundary
            if (segment->getRowKeyRange().getStart() == bp_key.toRowKeyValueRef())
                break;
            auto split_mode = use_logical_split ? DeltaMergeStore::SegmentSplitMode::Logical
                                                : DeltaMergeStore::SegmentSplitMode::Physical;
            auto [left, right] = store->segmentSplit(
                *dm_context,
                segment,
                DeltaMergeStore::SegmentSplitReason::ForegroundWrite,
                bp_key,
                split_mode);
            if (left)
                break;
        }
    }
}

std::vector<Int64> SimplePKTestBasic::getSegmentBreakpoints() const
{
    std::vector<Int64> breakpoints;
    std::unique_lock lock(store->read_write_mutex);
    if (store->segments.empty())
    {
        return breakpoints;
    }
    for (auto it = std::next(store->segments.cbegin()); it != store->segments.cend(); it++)
    {
        auto [start, end] = parseRange(it->second->getRowKeyRange());
        breakpoints.push_back(start);
    }
    return breakpoints;
}

RowKeyValue SimplePKTestBasic::buildRowKey(Int64 pk) const
{
    if (!is_common_handle)
        return RowKeyValue::fromHandle(pk);

    WriteBufferFromOwnString ss;
    ::DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
    ::DB::EncodeInt64(pk, ss);
    return RowKeyValue{true, std::make_shared<String>(ss.releaseStr()), pk};
}

RowKeyRange SimplePKTestBasic::buildRowRange(Int64 start, Int64 end) const
{
    return RowKeyRange(buildRowKey(start), buildRowKey(end), is_common_handle, 1);
}

std::pair<Int64, Int64> SimplePKTestBasic::parseRange(const RowKeyRange & range) const
{
    Int64 start_key, end_key;

    if (!is_common_handle)
    {
        start_key = range.getStart().int_value;
        end_key = range.getEnd().int_value;
        return {start_key, end_key};
    }

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

Block SimplePKTestBasic::fillBlock(const FillBlockOptions & options)
{
    auto start_key = options.range.first;
    auto end_key = options.range.second;

    RUNTIME_CHECK(start_key <= end_key);
    if (end_key == start_key)
        return Block{};
    version++;
    return DMTestEnv::prepareSimpleWriteBlock(
        start_key, //
        end_key,
        false,
        version,
        DMTestEnv::pk_name,
        EXTRA_HANDLE_COLUMN_ID,
        is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE,
        is_common_handle,
        1,
        true,
        options.is_deleted);
}

void SimplePKTestBasic::fill(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "fill [{}, {})", start_key, end_key);

    auto block = fillBlock({.range = {start_key, end_key}});
    store->write(*db_context, db_context->getSettingsRef(), block);
}

void SimplePKTestBasic::fillDelete(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "fillDelete [{}, {})", start_key, end_key);

    auto block = fillBlock({.range = {start_key, end_key}, .is_deleted = true});
    store->write(*db_context, db_context->getSettingsRef(), block);
}

void SimplePKTestBasic::flush(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "flush [{}, {})", start_key, end_key);

    auto range = buildRowRange(start_key, end_key);
    store->flushCache(*db_context, range, true);
}

void SimplePKTestBasic::flush()
{
    LOG_INFO(logger_op, "flushAll");

    auto range = RowKeyRange::newAll(is_common_handle, 1);
    store->flushCache(*db_context, range, true);
}

void SimplePKTestBasic::mergeDelta(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "mergeDelta [{}, {})", start_key, end_key);

    auto range = buildRowRange(start_key, end_key);
    while (!range.none())
    {
        auto processed_range = store->mergeDeltaBySegment(*db_context, range.start);
        RUNTIME_CHECK(processed_range.has_value());
        range.setStart(processed_range->end);
    }
}

void SimplePKTestBasic::mergeDelta()
{
    LOG_INFO(logger_op, "mergeDeltaAll");

    flush(); // as mergeDeltaBySegment always flush, so we also flush here.
    store->mergeDeltaAll(*db_context);
}

bool SimplePKTestBasic::merge(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "merge [{}, {})", start_key, end_key);

    std::vector<SegmentPtr> to_merge;
    auto range = buildRowRange(start_key, end_key);
    {
        std::shared_lock lock(store->read_write_mutex);
        while (!range.none())
        {
            auto [segment, is_empty] = store->getSegmentByStartKey(range.getStart(), true, true);
            to_merge.emplace_back(segment);
            range.setStart(segment->getRowKeyRange().end);
        }
    }

    if (to_merge.size() < 2)
        return false;

    auto merged = store->segmentMerge(*dm_context, to_merge, DeltaMergeStore::SegmentMergeReason::BackgroundGCThread);
    return merged != nullptr;
}

void SimplePKTestBasic::deleteRange(Int64 start_key, Int64 end_key)
{
    LOG_INFO(logger_op, "deleteRange [{}, {})", start_key, end_key);

    auto range = buildRowRange(start_key, end_key);
    store->deleteRange(*db_context, db_context->getSettingsRef(), range);
}

ExternalDTFileInfo genDMFile(DeltaMergeStorePtr store, DMContext & context, const Block & block)
{
    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto [store_path, file_id] = store->preAllocateIngestFile();

    auto dmfile = writeIntoNewDMFile(
        context,
        std::make_shared<ColumnDefines>(store->getTableColumns()),
        input_stream,
        file_id,
        store_path);

    store->preIngestFile(store_path, file_id, dmfile->getBytesOnDisk());

    const auto & pk_column = block.getByPosition(0).column;
    auto min_pk = pk_column->getInt(0);
    auto max_pk = pk_column->getInt(block.rows() - 1);
    HandleRange range(min_pk, max_pk + 1);
    auto handle_range = RowKeyRange::fromHandleRange(range);
    auto external_file = ExternalDTFileInfo{.id = file_id, .range = handle_range};
    return external_file;
}

void SimplePKTestBasic::ingestFiles(const IngestFilesOptions & options)
{
    LOG_INFO(
        logger_op,
        "ingestFiles range=[{}, {}), clear={}",
        options.range.first,
        options.range.second,
        options.clear);

    auto range = buildRowRange(options.range.first, options.range.second);

    std::vector<DM::ExternalDTFileInfo> external_files;
    external_files.reserve(options.blocks.size());
    for (const auto & block : options.blocks)
    {
        auto f = genDMFile(store, *dm_context, block);
        external_files.emplace_back(std::move(f));
    }

    store->ingestFiles(dm_context, range, external_files, options.clear);
}

size_t SimplePKTestBasic::getRowsN() const
{
    auto in = store->read(
        *db_context,
        db_context->getSettingsRef(),
        store->getTableColumns(),
        {RowKeyRange::newAll(is_common_handle, 1)},
        /* num_streams= */ 1,
        /* start_ts= */ std::numeric_limits<UInt64>::max(),
        EMPTY_FILTER,
        std::vector<RuntimeFilterPtr>{},
        0,
        "",
        /* keep_order= */ false,
        /* is_fast_scan= */ false,
        /* expected_block_size= */ 1024)[0];
    return getInputStreamNRows(in);
}

size_t SimplePKTestBasic::getRowsN(Int64 start_key, Int64 end_key) const
{
    auto in = store->read(
        *db_context,
        db_context->getSettingsRef(),
        store->getTableColumns(),
        {buildRowRange(start_key, end_key)},
        /* num_streams= */ 1,
        /* start_ts= */ std::numeric_limits<UInt64>::max(),
        EMPTY_FILTER,
        std::vector<RuntimeFilterPtr>{},
        0,
        "",
        /* keep_order= */ false,
        /* is_fast_scan= */ false,
        /* expected_block_size= */ 1024)[0];
    return getInputStreamNRows(in);
}

size_t SimplePKTestBasic::getRawRowsN() const
{
    auto in = store->readRaw(
        *db_context,
        db_context->getSettingsRef(),
        store->getTableColumns(),
        /* num_streams= */ 1,
        false)[0];
    return getInputStreamNRows(in);
}

bool SimplePKTestBasic::isFilled(Int64 start_key, Int64 end_key) const
{
    RUNTIME_CHECK(start_key <= end_key);
    return getRowsN(start_key, end_key) == static_cast<size_t>(end_key - start_key);
}

void SimplePKTestBasic::debugDumpAllSegments() const
{
    std::shared_lock lock(store->read_write_mutex);
    for (auto [key, segment] : store->segments)
    {
        UNUSED(key);
        LOG_INFO(logger, "debugDumpAllSegments: {}", segment->info());
    }
}


TEST_F(SimplePKTestBasic, FillAndRead)
try
{
    fill(0, 100);
    EXPECT_EQ(100, getRowsN(-50, 200));
    EXPECT_EQ(80, getRowsN(20, 200));

    fillDelete(40, 150);
    EXPECT_EQ(40, getRowsN(-50, 200));
}
CATCH


TEST_F(SimplePKTestBasic, SegmentBreakpoints)
try
{
    for (auto ch : {true, false})
    {
        is_common_handle = ch;
        reload();

        {
            ASSERT_EQ(store->segments.size(), 0);
            auto bps = getSegmentBreakpoints();
            ASSERT_EQ(bps.size(), 0);
        }
        {
            ensureSegmentBreakpoints({100, 10, -40, 500});
        }
        {
            ASSERT_EQ(store->segments.size(), 5);
            auto bps = getSegmentBreakpoints();
            ASSERT_EQ(bps.size(), 4);
            ASSERT_EQ(bps[0], -40);
            ASSERT_EQ(bps[1], 10);
            ASSERT_EQ(bps[2], 100);
            ASSERT_EQ(bps[3], 500);
        }
        {
            // One breakpoint is equal to a segment boundary, check whether it does not cause problems.
            ensureSegmentBreakpoints({30, 10});
        }
        {
            ASSERT_EQ(store->segments.size(), 6);
            auto bps = getSegmentBreakpoints();
            ASSERT_EQ(bps.size(), 5);
            ASSERT_EQ(bps[0], -40);
            ASSERT_EQ(bps[1], 10);
            ASSERT_EQ(bps[2], 30);
            ASSERT_EQ(bps[3], 100);
            ASSERT_EQ(bps[4], 500);
        }
    }
}
CATCH

TEST_F(SimplePKTestBasic, Merge)
try
{
    ensureSegmentBreakpoints({100, 10, -40, 500});
    ASSERT_EQ(std::vector<Int64>({-40, 10, 100, 500}), getSegmentBreakpoints());

    ASSERT_FALSE(merge(100, -100));
    ASSERT_FALSE(merge(100, 101));
    ASSERT_EQ(std::vector<Int64>({-40, 10, 100, 500}), getSegmentBreakpoints());

    ASSERT_TRUE(merge(90, 110));
    ASSERT_EQ(std::vector<Int64>({-40, 10, 500}), getSegmentBreakpoints());

    ASSERT_TRUE(merge(-100, 100));
    ASSERT_EQ(std::vector<Int64>({500}), getSegmentBreakpoints());
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB

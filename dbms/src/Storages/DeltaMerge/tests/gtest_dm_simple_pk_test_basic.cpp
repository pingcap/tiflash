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

namespace tests
{

void SimplePKTestBasic::reload()
{
    TiFlashStorageTestBasic::SetUp();

    version = 0;

    auto cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
    store = std::make_shared<DeltaMergeStore>(*db_context,
                                              false,
                                              "test",
                                              DB::base::TiFlashStorageTestBasic::getCurrentFullTestName(),
                                              101,
                                              *cols,
                                              (*cols)[0],
                                              is_common_handle,
                                              1,
                                              DeltaMergeStore::Settings());
    dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
    db_context->dropMinMaxIndexCache();
}

SegmentPtr SimplePKTestBasic::getSegmentAt(Int64 key) const
{
    auto row_key = buildRowKey(key);
    std::shared_lock lock(store->read_write_mutex);
    auto segment_it = store->segments.upper_bound(row_key.toRowKeyValueRef());
    RUNTIME_CHECK(segment_it != store->segments.end());
    auto segment = segment_it->second;
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
            SegmentPtr segment;
            {
                std::shared_lock lock(store->read_write_mutex);

                auto segment_it = store->segments.upper_bound(bp_key.toRowKeyValueRef());
                RUNTIME_CHECK(segment_it != store->segments.end());
                segment = segment_it->second;
            }
            // The segment is already break at the boundary
            if (compare(segment->getRowKeyRange().getStart(), bp_key.toRowKeyValueRef()) == 0)
                break;
            auto split_mode = use_logical_split ? DeltaMergeStore::SegmentSplitMode::Logical : DeltaMergeStore::SegmentSplitMode::Physical;
            auto [left, right] = store->segmentSplit(*dm_context, segment, DeltaMergeStore::SegmentSplitReason::ForegroundWrite, bp_key, split_mode);
            if (left)
                break;
        }
    }
}

std::vector<Int64> SimplePKTestBasic::getSegmentBreakpoints() const
{
    std::vector<Int64> breakpoints;
    std::unique_lock lock(store->read_write_mutex);
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

Block SimplePKTestBasic::prepareWriteBlock(Int64 start_key, Int64 end_key, bool is_deleted)
{
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
        is_deleted);
}

void SimplePKTestBasic::fill(Int64 start_key, Int64 end_key)
{
    LOG_INFO(
        logger_op,
        "fill [{}, {})",
        start_key,
        end_key);

    auto block = prepareWriteBlock(start_key, end_key);
    store->write(*db_context, db_context->getSettingsRef(), block);
}

void SimplePKTestBasic::fillDelete(Int64 start_key, Int64 end_key)
{
    LOG_INFO(
        logger_op,
        "fillDelete [{}, {})",
        start_key,
        end_key);

    auto block = prepareWriteBlock(start_key, end_key, /* delete */ true);
    store->write(*db_context, db_context->getSettingsRef(), block);
}

void SimplePKTestBasic::flush(Int64 start_key, Int64 end_key)
{
    LOG_INFO(
        logger_op,
        "flush [{}, {})",
        start_key,
        end_key);

    auto range = buildRowRange(start_key, end_key);
    store->flushCache(*db_context, range, true);
}

void SimplePKTestBasic::flush()
{
    LOG_INFO(
        logger_op,
        "flushAll");

    auto range = RowKeyRange::newAll(is_common_handle, 1);
    store->flushCache(*db_context, range, true);
}

void SimplePKTestBasic::mergeDelta(Int64 start_key, Int64 end_key)
{
    LOG_INFO(
        logger_op,
        "mergeDelta [{}, {})",
        start_key,
        end_key);

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
    LOG_INFO(
        logger_op,
        "mergeDeltaAll");

    flush(); // as mergeDeltaBySegment always flush, so we also flush here.
    store->mergeDeltaAll(*db_context);
}

void SimplePKTestBasic::deleteRange(Int64 start_key, Int64 end_key)
{
    LOG_INFO(
        logger_op,
        "deleteRange [{}, {})",
        start_key,
        end_key);

    auto range = buildRowRange(start_key, end_key);
    store->deleteRange(*db_context, db_context->getSettingsRef(), range);
}

size_t SimplePKTestBasic::getRowsN()
{
    const auto & columns = store->getTableColumns();
    auto in = store->read(
        *db_context,
        db_context->getSettingsRef(),
        columns,
        {RowKeyRange::newAll(is_common_handle, 1)},
        /* num_streams= */ 1,
        /* max_version= */ std::numeric_limits<UInt64>::max(),
        EMPTY_FILTER,
        "",
        /* keep_order= */ false,
        /* is_fast_scan= */ false,
        /* expected_block_size= */ 1024)[0];
    return getInputStreamNRows(in);
}

size_t SimplePKTestBasic::getRowsN(Int64 start_key, Int64 end_key)
{
    const auto & columns = store->getTableColumns();
    auto in = store->read(
        *db_context,
        db_context->getSettingsRef(),
        columns,
        {buildRowRange(start_key, end_key)},
        /* num_streams= */ 1,
        /* max_version= */ std::numeric_limits<UInt64>::max(),
        EMPTY_FILTER,
        "",
        /* keep_order= */ false,
        /* is_fast_scan= */ false,
        /* expected_block_size= */ 1024)[0];
    return getInputStreamNRows(in);
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
    FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
    SCOPE_EXIT({
        FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
    });

    for (auto ch : {true, false})
    {
        is_common_handle = ch;
        reload();

        {
            ASSERT_EQ(store->segments.size(), 1);
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


} // namespace tests
} // namespace DM
} // namespace DB

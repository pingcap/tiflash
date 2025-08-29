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

#include <Common/Logger.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{

class SegmentBitmapFilterTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentBitmapFilterTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    ColumnPtr hold_row_id;
    ColumnPtr hold_handle;
    RowKeyRanges read_ranges;

    void setRowKeyRange(Int64 begin, Int64 end)
    {
        auto itr = segments.find(SEG_ID);
        RUNTIME_CHECK(itr != segments.end(), SEG_ID);
        itr->second->rowkey_range = buildRowKeyRange(begin, end);
    }

    /*
    0----------------stable_rows----------------stable_rows + delta_rows <-- append
    | stable value space | delta value space ..........................  <-- append
    |--------------------|--ColumnFilePersisted--|ColumnFileInMemory...  <-- append
    |--------------------|-Tiny|DeleteRange|Big--|ColumnFileInMemory...  <-- append

    `seg_data`: s:[a, b)|d_tiny:[a, b)|d_tiny_del:[a, b)|d_big:[a, b)|d_dr:[a, b)|d_mem:[a, b)|d_mem_del
    - s: stable
    - d_tiny: delta ColumnFileTiny
    - d_del_tiny: delta ColumnFileTiny with delete flag
    - d_big: delta ColumnFileBig
    - d_dr: delta delete range

    Returns {row_id, handle}.
    */
    std::pair<const PaddedPODArray<UInt32> *, const PaddedPODArray<Int64> *> writeSegment(
        std::string_view seg_data,
        std::optional<std::pair<Int64, Int64>> rowkey_range = std::nullopt)
    {
        if (rowkey_range)
            setRowKeyRange(rowkey_range->first, rowkey_range->second);
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }
        hold_row_id = getSegmentRowId(SEG_ID, read_ranges);
        hold_handle = getSegmentHandle(SEG_ID, read_ranges);
        if (hold_row_id == nullptr)
        {
            RUNTIME_CHECK(hold_handle == nullptr);
            return {nullptr, nullptr};
        }
        else
        {
            RUNTIME_CHECK(hold_handle != nullptr);
            return {toColumnVectorDataPtr<UInt32>(hold_row_id), toColumnVectorDataPtr<Int64>(hold_handle)};
        }
    }

    void writeSegment(const SegDataUnit & unit)
    {
        const auto & type = unit.type;
        auto [begin, end] = unit.range;

        if (type == "d_mem")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
        }
        else if (type == "d_mem_del")
        {
            SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, end - begin, begin);
        }
        else if (type == "d_tiny")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
            SegmentTestBasic::flushSegmentCache(SEG_ID);
        }
        else if (type == "d_tiny_del")
        {
            SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, end - begin, begin);
            SegmentTestBasic::flushSegmentCache(SEG_ID);
        }
        else if (type == "d_big")
        {
            SegmentTestBasic::ingestDTFileIntoDelta(
                SEG_ID,
                end - begin,
                begin,
                false,
                unit.pack_size,
                /*check_range*/ false);
        }
        else if (type == "d_dr")
        {
            SegmentTestBasic::writeSegmentWithDeleteRange(SEG_ID, begin, end);
        }
        else if (type == "s")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
            if (unit.pack_size)
            {
                db_context->getSettingsRef().set("dt_segment_stable_pack_rows", *(unit.pack_size));
                reloadDMContext();
                ASSERT_EQ(dm_context->stable_pack_rows, *(unit.pack_size));
            }
            SegmentTestBasic::mergeSegmentDelta(SEG_ID);
        }
        else
        {
            RUNTIME_CHECK(false, type);
        }
    }

    struct TestCase
    {
        TestCase(
            std::string_view seg_data_,
            size_t expected_size_,
            std::string_view expected_row_id_,
            std::string_view expected_handle_,
            std::optional<std::pair<Int64, Int64>> rowkey_range_ = std::nullopt)
            : seg_data(seg_data_)
            , expected_size(expected_size_)
            , expected_row_id(expected_row_id_)
            , expected_handle(expected_handle_)
            , rowkey_range(rowkey_range_)
        {}
        std::string seg_data;
        size_t expected_size;
        std::string expected_row_id;
        std::string expected_handle;
        std::optional<std::pair<Int64, Int64>> rowkey_range;
    };

    void runTestCase(TestCase test_case)
    {
        auto [row_id, handle] = writeSegment(test_case.seg_data, test_case.rowkey_range);
        if (test_case.expected_size == 0)
        {
            ASSERT_EQ(nullptr, row_id);
            ASSERT_EQ(nullptr, handle);
        }
        else
        {
            ASSERT_EQ(test_case.expected_size, row_id->size());
            auto expected_row_id = genSequence<UInt32>(test_case.expected_row_id);
            ASSERT_TRUE(sequenceEqual(expected_row_id.data(), row_id->data(), test_case.expected_size));

            ASSERT_EQ(test_case.expected_size, handle->size());
            auto expected_handle = genSequence<Int64>(test_case.expected_handle);
            ASSERT_TRUE(sequenceEqual(expected_handle.data(), handle->data(), test_case.expected_size));
        }
    }
};

<<<<<<< HEAD
TEST_F(SegmentBitmapFilterTest, InMemory1)
try
{
    runTestCase(TestCase("d_mem:[0, 1000)", 1000, "[0, 1000)", "[0, 1000)"));
=======
void SegmentBitmapFilterTest::checkBitmap(const CheckBitmapOptions & opt)
{
    auto info = opt.toDebugString();
    auto [seg, snap] = getSegmentForRead(opt.seg_id);

    const auto read_ranges
        = shrinkRowKeyRanges(seg->getRowKeyRange(), opt.read_ranges.value_or(RowKeyRanges{seg->getRowKeyRange()}));

    const auto & rs_filter_results
        = opt.rs_filter_results.empty() ? loadPackFilterResults(snap, read_ranges) : opt.rs_filter_results;

    auto bitmap_filter_version_chain = seg->buildMVCCBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        rs_filter_results,
        opt.read_ts,
        DEFAULT_BLOCK_SIZE,
        /*enable_version_chain*/ true);

    auto bitmap_filter_delta_index = seg->buildMVCCBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        rs_filter_results,
        opt.read_ts,
        DEFAULT_BLOCK_SIZE,
        /*enable_version_chain*/ false);

    if (opt.expected_bitmap)
        ASSERT_EQ(bitmap_filter_version_chain->toDebugString(), *(opt.expected_bitmap))
            << fmt::format("{}, bitmap_filter_delta_index={}", info, bitmap_filter_delta_index->toDebugString());

    ASSERT_EQ(*bitmap_filter_delta_index, *bitmap_filter_version_chain) << fmt::format(
        "{}, bitmap_filter_delta_index={}, bitmap_filter_version_chain={}",
        info,
        bitmap_filter_delta_index->toDebugString(),
        bitmap_filter_version_chain->toDebugString());
}

INSTANTIATE_TEST_CASE_P(MVCC, SegmentBitmapFilterTest, /* is_common_handle */ ::testing::Bool());

UInt64 SegmentBitmapFilterTest::estimatedBytesOfInternalColumns(UInt64 start_ts, Int64 enable_version_chain)
{
    auto & ctx = db_context->getGlobalContext();
    auto & settings = ctx.getSettingsRef();
    Int64 original_enable_version_chain = settings.enable_version_chain;
    settings.set("enable_version_chain", std::to_string(enable_version_chain));
    SCOPE_EXIT({ settings.set("enable_version_chain", std::to_string(original_enable_version_chain)); });
    auto dm_ctx = DMContext::create(
        ctx,
        dm_context->path_pool,
        dm_context->storage_pool,
        dm_context->min_version,
        dm_context->keyspace_id,
        dm_context->physical_table_id,
        dm_context->pk_col_id,
        dm_context->is_common_handle,
        dm_context->rowkey_column_size,
        ctx.getSettingsRef(),
        dm_context->scan_context,
        dm_context->tracing_id);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto pack_filter_results = loadPackFilterResults(snap, {});
    return Segment::estimatedBytesOfInternalColumns(*dm_ctx, snap, pack_filter_results, start_ts);
}

TEST_P(SegmentBitmapFilterTest, InMemory1)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)",
            .expected_size = 1000,
            .expected_row_id = "[0, 1000)",
            .expected_handle = "[0, 1000)"},
        __LINE__);

    // Delta only
    auto bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 1);
    ASSERT_EQ(bytes, 17 * 1000);
    bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 0);
    ASSERT_EQ(bytes, 17 * 1000);
    bytes = estimatedBytesOfInternalColumns(0, 1);
    ASSERT_EQ(bytes, 17 * 1000);
    bytes = estimatedBytesOfInternalColumns(0, 0);
    ASSERT_EQ(bytes, 17 * 1000);
>>>>>>> c34abae6cd (Storages: Fix MVCC bitmap read bytes estimation (#10378))
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory2)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem:[0, 1000)", 1000, "[1000, 2000)", "[0, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory3)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem:[100, 200)", 1000, "[0, 100)|[1000, 1100)|[200, 1000)", "[0, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory4)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem:[-100, 100)", 1100, "[1000, 1200)|[100, 1000)", "[-100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory5)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem_del:[0, 1000)", 0, "", ""});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory6)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem_del:[100, 200)", 900, "[0, 100)|[200, 1000)", "[0, 100)|[200, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory7)
try
{
    runTestCase(TestCase{"d_mem:[0, 1000)|d_mem_del:[-100, 100)", 900, "[100, 1000)", "[100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Tiny1)
try
{
    runTestCase(TestCase{"d_tiny:[100, 500)|d_mem:[200, 1000)", 900, "[0, 100)|[400, 1200)", "[100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, TinyDel1)
try
{
    runTestCase(TestCase{
        "d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)",
        400,
        "[500, 600)|[0, 100)|[200, 400)",
        "[0, 200)|[300, 500)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, DeleteRange)
try
{
    runTestCase(TestCase{
        "d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)",
        390,
        "[0, 140)|[400, 450)|[200, 400)",
        "[100, 290)|[300, 500)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Big)
try
{
    runTestCase(TestCase{
        "d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)",
        900,
        "[0, 140)|[1150, 1200)|[440, 1150)",
        "[100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Stable1)
try
{
<<<<<<< HEAD
    runTestCase(TestCase{"s:[0, 1024)", 1024, "[0, 1024)", "[0, 1024)"});
=======
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)",
            .expected_size = 1024,
            .expected_row_id = "[0, 1024)",
            .expected_handle = "[0, 1024)"},
        __LINE__);

    // Stable only
    auto bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 0);
    ASSERT_EQ(bytes, 0);
    bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 1);
    ASSERT_EQ(bytes, 0);
    bytes = estimatedBytesOfInternalColumns(0, 0);
    ASSERT_GE(bytes, 17 * 1024);
    bytes = estimatedBytesOfInternalColumns(0, 1);
    ASSERT_GE(bytes, 17 * 1024);
>>>>>>> c34abae6cd (Storages: Fix MVCC bitmap read bytes estimation (#10378))
}
CATCH

TEST_F(SegmentBitmapFilterTest, Stable2)
try
{
    runTestCase(TestCase{"s:[0, 1024)|d_dr:[0, 1023)", 1, "[1023, 1024)", "[1023, 1024)"});
}
CATCH


TEST_F(SegmentBitmapFilterTest, Stable3)
try
{
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)",
        886,
        "[0, 128)|[256, 300)|[310, 1024)",
        "[0, 128)|[256, 300)|[310, 1024)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Mix)
try
{
<<<<<<< HEAD
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        946,
        "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
        "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"});
=======
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 946,
            .expected_row_id = "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            .expected_handle = "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__);

    // Delta + Stable
    auto bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 0);
    ASSERT_GE(bytes, 17 * (1024 + 10 + 55 + 7));
    bytes = estimatedBytesOfInternalColumns(std::numeric_limits<UInt64>::max(), 1);
    ASSERT_GE(bytes, 17 * (10 + 55 + 7));
    ASSERT_LT(bytes, 17 * (1024 + 10 + 55 + 7));

    bytes = estimatedBytesOfInternalColumns(0, 0);
    ASSERT_GE(bytes, 17 * (1024 + 10 + 55 + 7));
    bytes = estimatedBytesOfInternalColumns(0, 1);
    ASSERT_GE(bytes, 17 * (1024 + 10 + 55 + 7));
>>>>>>> c34abae6cd (Storages: Fix MVCC bitmap read bytes estimation (#10378))
}
CATCH

TEST_F(SegmentBitmapFilterTest, Ranges)
try
{
    read_ranges.emplace_back(buildRowKeyRange(222, 244));
    read_ranges.emplace_back(buildRowKeyRange(300, 303));
    read_ranges.emplace_back(buildRowKeyRange(555, 666));
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        136,
        "[1056, 1078)|[1091, 1094)|[555, 666)",
        "[222, 244)|[300, 303)|[555, 666)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, LogicalSplit)
try
{
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        946,
        "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
        "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"});

    auto new_seg_id = splitSegmentAt(SEG_ID, 512, Segment::SplitMode::Logical);

    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));

    auto left_handle = getSegmentHandle(SEG_ID, {});
    const auto * left_h = toColumnVectorDataPtr<Int64>(left_handle);
    auto expected_left_handle = genSequence<Int64>("[0, 128)|[200, 255)|[256, 305)|[310, 512)");
    ASSERT_EQ(expected_left_handle.size(), left_h->size());
    ASSERT_TRUE(sequenceEqual(expected_left_handle.data(), left_h->data(), left_h->size()));

    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto * left_r = toColumnVectorDataPtr<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 512)");
    ASSERT_EQ(expected_left_row_id.size(), left_r->size());
    ASSERT_TRUE(sequenceEqual(expected_left_row_id.data(), left_r->data(), left_r->size()));

    auto right_handle = getSegmentHandle(*new_seg_id, {});
    const auto * right_h = toColumnVectorDataPtr<Int64>(right_handle);
    auto expected_right_handle = genSequence<Int64>("[512, 1024)");
    ASSERT_EQ(expected_right_handle.size(), right_h->size());
    ASSERT_TRUE(sequenceEqual(expected_right_handle.data(), right_h->data(), right_h->size()));

    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto * right_r = toColumnVectorDataPtr<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[512, 1024)");
    ASSERT_EQ(expected_right_row_id.size(), right_r->size());
    ASSERT_TRUE(sequenceEqual(expected_right_row_id.data(), right_r->data(), right_r->size()));
}
CATCH

TEST_F(SegmentBitmapFilterTest, CleanStable)
{
    writeSegment("d_mem:[0, 20000)|d_mem:[30000, 35000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 25000);
    auto bitmap_filter = seg->buildBitmapFilterStableOnly(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        EMPTY_RS_OPERATOR,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_NE(bitmap_filter, nullptr);
    std::string expect_result;
    expect_result.append(std::string(25000, '1'));
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
}

TEST_F(SegmentBitmapFilterTest, NotCleanStable)
{
    writeSegment("d_mem:[0, 10000)|d_mem:[5000, 15000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 20000);
    {
        auto bitmap_filter = seg->buildBitmapFilterStableOnly(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            EMPTY_RS_OPERATOR,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_NE(bitmap_filter, nullptr);
        std::string expect_result;
        expect_result.append(std::string(5000, '1'));
        for (int i = 0; i < 5000; i++)
        {
            expect_result.append(std::string("01"));
        }
        expect_result.append(std::string(5000, '1'));
        ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
    }
    {
        // Stale read
        ASSERT_EQ(version, 2);
        auto bitmap_filter = seg->buildBitmapFilterStableOnly(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            EMPTY_RS_OPERATOR,
            1,
            DEFAULT_BLOCK_SIZE);
        ASSERT_NE(bitmap_filter, nullptr);
        std::string expect_result;
        expect_result.append(std::string(5000, '1'));
        for (int i = 0; i < 5000; i++)
        {
            expect_result.append(std::string("10"));
        }
        expect_result.append(std::string(5000, '0'));
        ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
    }
}

TEST_F(SegmentBitmapFilterTest, StableRange)
{
    writeSegment("d_mem:[0, 50000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 50000);

    auto bitmap_filter = seg->buildBitmapFilterStableOnly(
        *dm_context,
        snap,
        {buildRowKeyRange(10000, 50000)}, // [10000, 50000)
        EMPTY_RS_OPERATOR,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_NE(bitmap_filter, nullptr);
    std::string expect_result;
    // [0, 10000) is filtered by range.
    expect_result.append(std::string(10000, '0'));
    expect_result.append(std::string(40000, '1'));
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
}

TEST_F(SegmentBitmapFilterTest, StableLogicalSplit)
try
{
    writeSegment("d_mem:[0, 50000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 50000);

    auto new_seg_id = splitSegmentAt(SEG_ID, 25000, Segment::SplitMode::Logical);

    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));

    auto left_handle = getSegmentHandle(SEG_ID, {});
    const auto * left_h = toColumnVectorDataPtr<Int64>(left_handle);
    auto expected_left_handle = genSequence<Int64>("[0, 25000)");
    ASSERT_EQ(expected_left_handle.size(), left_h->size());
    ASSERT_TRUE(sequenceEqual(expected_left_handle.data(), left_h->data(), left_h->size()));

    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto * left_r = toColumnVectorDataPtr<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 25000)");
    ASSERT_EQ(expected_left_row_id.size(), left_r->size());
    ASSERT_TRUE(sequenceEqual(expected_left_row_id.data(), left_r->data(), left_r->size()));

    auto right_handle = getSegmentHandle(*new_seg_id, {});
    const auto * right_h = toColumnVectorDataPtr<Int64>(right_handle);
    auto expected_right_handle = genSequence<Int64>("[25000, 50000)");
    ASSERT_EQ(expected_right_handle.size(), right_h->size());
    ASSERT_TRUE(sequenceEqual(expected_right_handle.data(), right_h->data(), right_h->size()));

    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto * right_r = toColumnVectorDataPtr<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[25000, 50000)");
    ASSERT_EQ(expected_right_row_id.size(), right_r->size());
    ASSERT_TRUE(sequenceEqual(expected_right_row_id.data(), right_r->data(), right_r->size()));
}
CATCH

TEST_F(SegmentBitmapFilterTest, BigPart)
try
{
    // For ColumnFileBig, only packs that intersection with the rowkey range will be considered in BitmapFilter.
    // Packs in rowkey_range: [270, 280)|[280, 290)|[290, 300)
    runTestCase(TestCase{
        /*seg_data*/ "d_big:[250, 1000):10",
        /*expected_size*/ 20,
        /*expected_row_id*/ "[5, 25)",
        /*expected_handle*/ "[275, 295)",
        /*rowkey_range*/ std::pair<Int64, Int64>{275, 295}});

    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        nullptr,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(bitmap_filter->size(), 30);
    ASSERT_EQ(bitmap_filter->count(), 20); // `count()` returns the number of bit has been set.
    ASSERT_EQ(bitmap_filter->toDebugString(), "000001111111111111111111100000");
}
CATCH

TEST_F(SegmentBitmapFilterTest, StablePart)
try
{
    runTestCase(TestCase{
        /*seg_data*/ "s:[250, 1000):10",
        /*expected_size*/ 750,
        /*expected_row_id*/ "[0, 750)",
        /*expected_handle*/ "[250, 1000)"});

    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->stable->getDMFilesPacks(), 75);
    }

    // For Stable, all packs of DMFile will be considered in BitmapFilter.
    setRowKeyRange(275, 295); // Shrinking range
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        nullptr,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(bitmap_filter->size(), 750);
    ASSERT_EQ(bitmap_filter->count(), 20); // `count()` returns the number of bit has been set.
    ASSERT_EQ(
        bitmap_filter->toDebugString(),
        "00000000000000000000000001111111111111111111100000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
}
CATCH

} // namespace DB::DM::tests

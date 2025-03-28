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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/tests/gtest_segment_bitmap.h>

using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{

void SegmentBitmapFilterTest::SetUp()
{
    is_common_handle = GetParam();
    SegmentTestBasic::SetUp(SegmentTestOptions{.is_common_handle = is_common_handle});
}

void SegmentBitmapFilterTest::setRowKeyRange(Int64 begin, Int64 end, bool including_right_boundary)
{
    auto itr = segments.find(SEG_ID);
    RUNTIME_CHECK(itr != segments.end(), SEG_ID);
    itr->second->rowkey_range = buildRowKeyRange(begin, end, is_common_handle, including_right_boundary);
}

void SegmentBitmapFilterTest::writeSegmentGeneric(
    std::string_view seg_data,
    std::optional<std::tuple<Int64, Int64, bool>> seg_rowkey_range)
{
    if (is_common_handle)
        writeSegment<String>(seg_data, seg_rowkey_range);
    else
        writeSegment<Int64>(seg_data, seg_rowkey_range);
}

template <typename HandleType>
void SegmentBitmapFilterTest::writeSegment(
    std::string_view seg_data,
    std::optional<std::tuple<Int64, Int64, bool>> seg_rowkey_range)
{
    if (seg_rowkey_range)
    {
        const auto & [left, right, including_right_boundary] = *seg_rowkey_range;
        setRowKeyRange(left, right, including_right_boundary);
    }
    auto seg_data_units = parseSegData(seg_data);
    for (const auto & unit : seg_data_units)
    {
        writeSegment(unit);
    }
}

void SegmentBitmapFilterTest::writeSegment(const SegDataUnit & unit)
{
    const auto & type = unit.type;
    auto [begin, end, including_right_boundary] = unit.range;
    const auto write_count = end - begin + including_right_boundary;
    if (type == "d_mem")
    {
        SegmentTestBasic::writeToCache(SEG_ID, write_count, begin, unit.shuffle, unit.ts);
    }
    else if (type == "d_mem_del")
    {
        SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, write_count, begin);
    }
    else if (type == "d_tiny")
    {
        SegmentTestBasic::writeSegment(SEG_ID, write_count, begin, unit.shuffle, unit.ts);
        SegmentTestBasic::flushSegmentCache(SEG_ID);
    }
    else if (type == "d_tiny_del")
    {
        SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, write_count, begin);
        SegmentTestBasic::flushSegmentCache(SEG_ID);
    }
    else if (type == "d_big")
    {
        SegmentTestBasic::ingestDTFileIntoDelta(
            SEG_ID,
            write_count,
            begin,
            false,
            unit.pack_size,
            /*check_range*/ false);
    }
    else if (type == "d_dr")
    {
        SegmentTestBasic::writeSegmentWithDeleteRange(SEG_ID, begin, end, is_common_handle, including_right_boundary);
    }
    else if (type == "s")
    {
        SegmentTestBasic::writeSegment(SEG_ID, write_count, begin, unit.shuffle, unit.ts);
        SegmentTestBasic::mergeSegmentDelta(SEG_ID, /*check_rows*/ true, unit.pack_size);
    }
    else if (type == "compact_delta")
    {
        SegmentTestBasic::compactSegmentDelta(SEG_ID);
    }
    else if (type == "flush_cache")
    {
        SegmentTestBasic::flushSegmentCache(SEG_ID);
    }
    else if (type == "merge_delta")
    {
        SegmentTestBasic::mergeSegmentDelta(SEG_ID, /*check_rows*/ true, unit.pack_size);
    }
    else
    {
        RUNTIME_CHECK(false, type);
    }
}

void SegmentBitmapFilterTest::runTestCaseGeneric(TestCase test_case, int caller_line)
{
    if (is_common_handle)
        runTestCase<String>(test_case, caller_line);
    else
        runTestCase<Int64>(test_case, caller_line);
}

template <typename HandleType>
void SegmentBitmapFilterTest::runTestCase(TestCase test_case, int caller_line)
{
    auto info = fmt::format("caller_line={}", caller_line);

    writeSegment<HandleType>(test_case.seg_data, test_case.seg_rowkey_range);

    // Verify row_id and handles by using DeltaIndex.
    auto col_row_id = getSegmentRowId(SEG_ID, test_case.read_ranges);
    auto col_handle = getSegmentHandle(SEG_ID, test_case.read_ranges);
    if (test_case.expected_size == 0)
    {
        ASSERT_EQ(nullptr, col_row_id) << info;
        ASSERT_EQ(nullptr, col_handle) << info;
    }
    else
    {
        auto expected_row_id = genSequence<UInt32>(test_case.expected_row_id);
        auto row_id = ColumnView<UInt32>(*col_row_id);
        ASSERT_TRUE(sequenceEqual(expected_row_id, row_id)) << info;

        auto expected_handle = genHandleSequence<HandleType>(test_case.expected_handle);
        auto handle = ColumnView<HandleType>(*col_handle);
        ASSERT_TRUE(sequenceEqual(expected_handle, handle)) << info;
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = caller_line,
        .expected_bitmap = test_case.expected_bitmap,
    });
}

DMFilePackFilterResults SegmentBitmapFilterTest::loadPackFilterResults(
    const SegmentSnapshotPtr & snap,
    const RowKeyRanges & ranges)
{
    DMFilePackFilterResults results;
    results.reserve(snap->stable->getDMFiles().size());
    for (const auto & file : snap->stable->getDMFiles())
    {
        auto pack_filter = DMFilePackFilter::loadFrom(*dm_context, file, true, ranges, EMPTY_RS_OPERATOR, {});
        results.push_back(pack_filter);
    }
    return results;
}

void SegmentBitmapFilterTest::checkHandle(PageIdU64 seg_id, std::string_view seq_ranges, int caller_line)
{
    auto info = fmt::format("caller_line={}", caller_line);
    auto handle = getSegmentHandle(seg_id, {});
    if (is_common_handle)
    {
        auto expected_handle = genHandleSequence<String>(seq_ranges);
        ASSERT_TRUE(sequenceEqual(expected_handle, ColumnView<String>{*handle})) << info;
    }
    else
    {
        auto expected_handle = genHandleSequence<Int64>(seq_ranges);
        ASSERT_TRUE(sequenceEqual(expected_handle, ColumnView<Int64>{*handle})) << info;
    }
}

void SegmentBitmapFilterTest::checkBitmap(const CheckBitmapOptions & opt)
{
    auto info = opt.toDebugString();
    auto [seg, snap] = getSegmentForRead(opt.seg_id);

    const auto read_ranges
        = shrinkRowKeyRanges(seg->getRowKeyRange(), opt.read_ranges.value_or(RowKeyRanges{seg->getRowKeyRange()}));

    const auto & rs_filter_results
        = opt.rs_filter_results.empty() ? loadPackFilterResults(snap, read_ranges) : opt.rs_filter_results;

    auto bitmap_filter_version_chain = seg->buildBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        rs_filter_results,
        opt.read_ts,
        DEFAULT_BLOCK_SIZE,
        enable_version_chain);

    auto bitmap_filter_delta_index = seg->buildBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        rs_filter_results,
        opt.read_ts,
        DEFAULT_BLOCK_SIZE,
        !enable_version_chain);

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
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory2)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[0, 1000)",
            .expected_size = 1000,
            .expected_row_id = "[1000, 2000)",
            .expected_handle = "[0, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory3)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[100, 200)",
            .expected_size = 1000,
            .expected_row_id = "[0, 100)|[1000, 1100)|[200, 1000)",
            .expected_handle = "[0, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory4)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[-100, 100)",
            .expected_size = 1100,
            .expected_row_id = "[1000, 1200)|[100, 1000)",
            .expected_handle = "[-100, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory5)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[0, 1000)",
            .expected_size = 0,
            .expected_row_id = "",
            .expected_handle = ""},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory6)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[100, 200)",
            .expected_size = 900,
            .expected_row_id = "[0, 100)|[200, 1000)",
            .expected_handle = "[0, 100)|[200, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory7)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[-100, 100)",
            .expected_size = 900,
            .expected_row_id = "[100, 1000)",
            .expected_handle = "[100, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Tiny1)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_mem:[200, 1000)",
            .expected_size = 900,
            .expected_row_id = "[0, 100)|[400, 1200)",
            .expected_handle = "[100, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, TinyDel1)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)",
            .expected_size = 400,
            .expected_row_id = "[500, 600)|[0, 100)|[200, 400)",
            .expected_handle = "[0, 200)|[300, 500)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)",
            .expected_size = 390,
            .expected_row_id = "[0, 140)|[400, 450)|[200, 400)",
            .expected_handle = "[100, 290)|[300, 500)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Big)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)",
            .expected_size = 900,
            .expected_row_id = "[0, 140)|[1150, 1200)|[440, 1150)",
            .expected_handle = "[100, 1000)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable1)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)",
            .expected_size = 1024,
            .expected_row_id = "[0, 1024)",
            .expected_handle = "[0, 1024)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable2)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[0, 1023)",
            .expected_size = 1,
            .expected_row_id = "[1023, 1024)",
            .expected_handle = "[1023, 1024)"},
        __LINE__);
}
CATCH


TEST_P(SegmentBitmapFilterTest, Stable3)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)",
            .expected_size = 886,
            .expected_row_id = "[0, 128)|[256, 300)|[310, 1024)",
            .expected_handle = "[0, 128)|[256, 300)|[310, 1024)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Mix)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 946,
            .expected_row_id = "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            .expected_handle = "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Ranges)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 136,
            .expected_row_id = "[1056, 1078)|[1091, 1094)|[555, 666)",
            .expected_handle = "[222, 244)|[300, 303)|[555, 666)",
            .read_ranges = {
                buildRowKeyRange(222, 244, is_common_handle),
                buildRowKeyRange(300, 303, is_common_handle),
                buildRowKeyRange(555, 666, is_common_handle),
            },
        },
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, LogicalSplit)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 946,
            .expected_row_id = "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            .expected_handle = "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__);

    auto new_seg_id = splitSegmentAt(SEG_ID, 512, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));
    checkHandle(SEG_ID, "[0, 128)|[200, 255)|[256, 305)|[310, 512)", __LINE__);
    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto & left_r = toColumnVectorData<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 512)");
    ASSERT_TRUE(sequenceEqual(expected_left_row_id, left_r));

    checkHandle(*new_seg_id, "[512, 1024)", __LINE__);
    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto & right_r = toColumnVectorData<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[512, 1024)");
    ASSERT_TRUE(sequenceEqual(expected_right_row_id, right_r));

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });

    checkBitmap(CheckBitmapOptions{
        .seg_id = *new_seg_id,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, CleanStable)
{
    writeSegmentGeneric("d_mem:[0, 20000)|d_mem:[30000, 35000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 25000);
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = String(25000, '1'),
    });
}

TEST_P(SegmentBitmapFilterTest, NotCleanStable)
{
    writeSegmentGeneric("d_mem:[0, 10000)|d_mem:[5000, 15000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 20000);
    {
        String expect_result;
        expect_result.append(std::string(5000, '1'));
        for (int i = 0; i < 5000; i++)
            expect_result.append(std::string("01"));
        expect_result.append(std::string(5000, '1'));
        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .expected_bitmap = expect_result,
        });
    }

    {
        // Stale read
        ASSERT_EQ(version, 2);
        String expect_result;
        expect_result.append(String(5000, '1'));
        for (int i = 0; i < 5000; i++)
            expect_result.append(String("10"));
        expect_result.append(String(5000, '0'));
        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ts = 1,
            .expected_bitmap = expect_result,
        });
    }
}

TEST_P(SegmentBitmapFilterTest, StableRange)
{
    writeSegmentGeneric("d_mem:[0, 50000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 50000);

    String expect_result;
    // [0, 10000) is filtered by range.
    expect_result.append(String(10000, '0'));
    expect_result.append(String(40000, '1'));
    const auto ranges = RowKeyRanges{buildRowKeyRange(10000, 50000, is_common_handle)}; // [10000, 50000)
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ranges = ranges,
        .expected_bitmap = expect_result,
    });
}

TEST_P(SegmentBitmapFilterTest, StableLogicalSplit)
try
{
    writeSegmentGeneric("d_mem:[0, 50000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 50000);

    auto new_seg_id = splitSegmentAt(SEG_ID, 25000, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));

    checkHandle(SEG_ID, "[0, 25000)", __LINE__);
    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto & left_r = toColumnVectorData<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 25000)");
    ASSERT_TRUE(sequenceEqual(expected_left_row_id, left_r));

    checkHandle(*new_seg_id, "[25000, 50000)", __LINE__);
    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto & right_r = toColumnVectorData<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[25000, 50000)");
    ASSERT_TRUE(sequenceEqual(expected_right_row_id, right_r));

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = *new_seg_id,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, BigPart)
try
{
    // For ColumnFileBig, only packs that intersection with the rowkey range will be considered in BitmapFilter.
    // Packs in rowkey_range: [270, 280)|[280, 290)|[290, 300)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_big:[250, 1000):pack_size_10",
            .expected_size = 20,
            .expected_row_id = "[5, 25)",
            .expected_handle = "[275, 295)",
            .seg_rowkey_range = std::tuple<Int64, Int64, bool>{275, 295, false},
            .expected_bitmap = "000001111111111111111111100000",
        },
        __LINE__);
}
CATCH

TEST_P(SegmentBitmapFilterTest, StablePart)
try
{
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[250, 1000):pack_size_10",
            .expected_size = 750,
            .expected_row_id = "[0, 750)",
            .expected_handle = "[250, 1000)"},
        __LINE__);

    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->stable->getDMFilesPacks(), 75);
    }

    // For Stable, all packs of DMFile will be considered in BitmapFilter.
    setRowKeyRange(275, 295, false); // Shrinking range
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap
        = "000000000000000000000000011111111111111111111000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, testSkipPackStableOnly)
{
    std::string expect_result;
    expect_result.append(std::string(200, '0'));
    expect_result.append(std::string(300, '1'));
    for (size_t i = 0; i < 500; i++)
        expect_result.append(std::string("01"));
    expect_result.append(std::string(500, '1'));
    expect_result.append(std::string(500, '0'));
    for (size_t pack_rows : {1, 2, 10, 100, 8192})
    {
        db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_rows;
        db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = pack_rows;
        reloadDMContext();

        version = 0;
        writeSegmentGeneric("d_mem:[0, 1000)|d_mem:[500, 1500)|d_mem:[1500, 2000)");
        mergeSegmentDelta(SEG_ID, true);
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->getDelta()->getRows(), 0);
        ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
        ASSERT_EQ(seg->getStable()->getRows(), 2500);

        auto ranges = std::vector<RowKeyRange>{buildRowKeyRange(200, 2000, is_common_handle)};
        auto pack_filter_results = loadPackFilterResults(snap, ranges);

        if (pack_rows == 1)
        {
            ASSERT_EQ(version, 3);
            const auto & dmfiles = snap->stable->getDMFiles();
            auto [skipped_ranges, new_pack_filter_results]
                = DMFilePackFilter::getSkippedRangeAndFilterForBitmapStableOnly(
                    *dm_context,
                    dmfiles,
                    pack_filter_results,
                    2);
            // [200, 500), [1500, 2000)
            ASSERT_EQ(skipped_ranges.size(), 2);
            ASSERT_EQ(skipped_ranges[0], DMFilePackFilter::Range(200, 300));
            ASSERT_EQ(skipped_ranges[1], DMFilePackFilter::Range(1500, 500));
            ASSERT_EQ(new_pack_filter_results.size(), 1);
            const auto & pack_res = new_pack_filter_results[0]->getPackRes();
            ASSERT_EQ(pack_res.size(), 2000);
            // [0, 200) is not in range, [200, 500) is skipped.
            for (size_t i = 0; i < 500; ++i)
                ASSERT_EQ(pack_res[i], RSResult::None);
            // Not clean
            for (size_t i = 500; i < 1000; ++i)
                ASSERT_EQ(pack_res[i], RSResult::Some);
            for (size_t i = 1000; i < 1500; ++i)
                ASSERT_EQ(pack_res[i], RSResult::None);
            // min version > start_ts
            for (size_t i = 1500; i < 2000; ++i)
                ASSERT_EQ(pack_res[i], RSResult::Some);
        }

        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ts = 2,
            .read_ranges = ranges,
            .expected_bitmap = expect_result,
            .rs_filter_results = pack_filter_results,
        });

        deleteRangeSegment(SEG_ID);
    }
}

TEST_P(SegmentBitmapFilterTest, testSkipPackNormal)
{
    std::string expect_result;
    expect_result.append(std::string(50, '0'));
    expect_result.append(std::string(450, '1'));
    for (size_t i = 0; i < 500; i++)
        expect_result.append(std::string("01"));
    expect_result.append(std::string(1000, '1'));
    expect_result.append(std::string(16, '1'));

    expect_result[99] = '0';
    expect_result[200] = '0';
    for (size_t i = 301; i < 315; ++i)
        expect_result[i] = '0';
    for (size_t i = 355; i < 370; ++i)
        expect_result[i] = '0';
    for (size_t i = 409; i < 481; ++i)
        expect_result[i] = '0';

    for (size_t pack_rows : {1, 2, 10, 100, 8192})
    {
        db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_rows;
        db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = pack_rows;
        reloadDMContext();

        version = 0;
        writeSegmentGeneric("d_mem:[0, 1000)|d_mem:[500, 1500)|d_mem:[1500, 2000)");
        mergeSegmentDelta(SEG_ID, true);
        writeSegmentGeneric("d_tiny:[99, 100)|d_dr:[355, 370)|d_dr:[409, 481)|d_mem:[200, 201)|d_mem:[301, 315)");
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->getDelta()->getRows(), 16);
        ASSERT_EQ(seg->getDelta()->getDeletes(), 2);
        ASSERT_EQ(seg->getStable()->getRows(), 2500);

        auto ranges = std::vector<RowKeyRange>{buildRowKeyRange(50, 2000, is_common_handle)};
        auto pack_filter_results = loadPackFilterResults(snap, ranges);
        UInt64 start_ts = 6;
        if (pack_rows == 10)
        {
            ASSERT_EQ(version, 6);
            const auto & dmfiles = snap->stable->getDMFiles();
            ColumnDefines columns_to_read{
                getExtraHandleColumnDefine(seg->is_common_handle),
            };
            auto read_info = seg->getReadInfo(*dm_context, columns_to_read, snap, ranges, ReadTag::MVCC, start_ts);
            auto [skipped_ranges, new_pack_filter_results] = DMFilePackFilter::getSkippedRangeAndFilterForBitmapNormal(
                *dm_context,
                dmfiles,
                pack_filter_results,
                start_ts,
                read_info.index_begin,
                read_info.index_end);

            std::vector<std::tuple<size_t, size_t, bool>> skip_ranges_result = {
                // [50, 90)
                {50, 40, false},
                // Due to changing 99, [90, 100) pack can not be skipped.
                // However, [100, 110) pack also can not be skipped because there is a preceding delta row,
                // which can be optimized further if we can identify whether the rowkeys are the same.
                // [110, 200)
                {110, 90, false},
                // Due to changing 200, [200, 210) pack can not be skipped.
                // [210, 300)
                {210, 90, false},
                // Due to changing [301, 315), [300, 310) and [310, 320) packs can not be skipped.
                // [320, 350)
                {320, 30, false},
                // Due to deleting [355, 370), [350, 360) pack can not be skipped.
                // [360, 370) pack can be skipped due to be totally deleted.
                {360, 10, true},
                // [370, 400)
                {370, 30, false},
                // Due to deleting [409, 481), [400, 410) and [480, 490) packs can not be skipped.
                // Packs between [410, 480) can be skipped due to be totally deleted.
                {410, 70, true},
                // [490, 500)
                {490, 10, false},
                // [1500, 2500)
                {1500, 1000, false},
            };

            size_t skipped_ranges_idx = 0;
            const auto & pack_res = new_pack_filter_results[0]->getPackRes();
            ASSERT_EQ(pack_filter_results.size(), 1);
            ASSERT_EQ(pack_res.size(), 250);
            std::vector<UInt8> flag(250);
            for (auto [offset, limit, is_delete] : skip_ranges_result)
            {
                if (!is_delete)
                    ASSERT_EQ(skipped_ranges.at(skipped_ranges_idx++), DMFilePackFilter::Range(offset, limit));

                for (size_t i = 0; i < limit; i += 10)
                {
                    size_t pack_id = (offset + i) / 10;
                    ASSERT_EQ(pack_res.at(pack_id), RSResult::None);
                    flag[pack_id] = 1;
                }
            }
            ASSERT_EQ(skipped_ranges_idx, skipped_ranges.size());
            // Check if other pack results are the same as the original pack results
            ASSERT_EQ(new_pack_filter_results.size(), 1);
            const auto & original_pack_res = pack_filter_results[0]->getPackRes();
            ASSERT_EQ(original_pack_res.size(), 250);
            for (size_t i = 0; i < 250; ++i)
            {
                if (flag[i])
                    continue;
                ASSERT_EQ(original_pack_res[i], pack_res[i]);
            }
        }

        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ts = start_ts,
            .read_ranges = ranges,
            .expected_bitmap = expect_result,
            .rs_filter_results = pack_filter_results,
        });

        deleteRangeSegment(SEG_ID);
    }
}

TEST_P(SegmentBitmapFilterTest, Int64Boundary)
try
{
    writeSegmentGeneric("d_mem:[-9223372036854775808, -9223372036854775800):shuffle|d_mem:[9223372036854775800, "
                        "9223372036854775807]:shuffle");
    {
        auto ranges = RowKeyRanges{
            buildRowKeyRange(std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max(), is_common_handle)};
        if (!is_common_handle)
            ASSERT_TRUE(ranges[0].isStartInfinite());
        ASSERT_FALSE(ranges[0].isEndInfinite());
        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ranges = ranges,
        });
    }

    {
        auto ranges = RowKeyRanges{RowKeyRange::newAll(is_common_handle, 1)};
        ASSERT_TRUE(ranges[0].isStartInfinite());
        ASSERT_TRUE(ranges[0].isEndInfinite());
        checkBitmap(CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ranges = ranges,
        });
    }
}
CATCH

TEST_P(SegmentBitmapFilterTest, Range_Stable)
try
{
    writeSegmentGeneric("s:[250, 1000):pack_size_50");
    checkBitmap(
        CheckBitmapOptions {
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ranges = RowKeyRanges{
                buildRowKeyRange(318, 520, is_common_handle),
                buildRowKeyRange(618, 737, is_common_handle),
                buildRowKeyRange(918, 998, is_common_handle),
            },
        }
    );
}
CATCH

TEST_P(SegmentBitmapFilterTest, Range_CFBig)
try
{
    writeSegmentGeneric("d_big:[250, 1000):pack_size_50", std::tuple{388, 888, false});
    checkBitmap(
        CheckBitmapOptions{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ranges = RowKeyRanges{
                buildRowKeyRange(318, 520, is_common_handle),
                buildRowKeyRange(618, 737, is_common_handle),
                buildRowKeyRange(818, 998, is_common_handle),
            },
        }
    );
}
CATCH

TEST_P(SegmentBitmapFilterTest, Range_CFTinyOrMem)
try
{
    writeSegmentGeneric("d_mem:[115, 277):shuffle|d_tiny:[140, 250):shuffle");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ranges = RowKeyRanges{buildRowKeyRange(120, 260, is_common_handle)}});
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange1)
try
{
    writeSegmentGeneric("s:[10, 500):pack_size_10|d_dr:[5, 73)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange2)
try
{
    writeSegmentGeneric("s:[10, 500):pack_size_10|d_dr:[5, 73)|d_tiny:[60, 83):shuffle");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange3)
try
{
    writeSegmentGeneric("s:[10, 500):pack_size_10|d_dr:[5, 73)|d_tiny:[60, 83):shuffle|d_dr:[70, 100)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange4)
try
{
    writeSegmentGeneric(
        "d_mem:[-9223372036854775808, -9223372036854775800):shuffle|d_mem:[9223372036854775800, "
        "9223372036854775807]:shuffle|"
        "d_dr:[-9223372036854775808, -9223372036854775804)|d_dr:[9223372036854775804, 9223372036854775807)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange4_1)
try
{
    writeSegmentGeneric(
        "d_mem:[-9223372036854775808, -9223372036854775800)|d_mem:[9223372036854775800, 9223372036854775807]|"
        "d_dr:[-9223372036854775808, -9223372036854775804)|d_dr:[9223372036854775804, 9223372036854775807)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "0000111111110001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange5)
try
{
    writeSegmentGeneric(
        "d_mem:[-9223372036854775808, -9223372036854775800):shuffle|d_mem:[9223372036854775800, "
        "9223372036854775807]:shuffle|"
        "d_dr:[-9223372036854775808, -9223372036854775804)|d_dr:[9223372036854775804, 9223372036854775807]");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange5_1)
try
{
    writeSegmentGeneric(
        "d_mem:[-9223372036854775808, -9223372036854775800)|d_mem:[9223372036854775800, 9223372036854775807]|"
        "d_dr:[-9223372036854775808, -9223372036854775804)|d_dr:[9223372036854775804, 9223372036854775807]");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "0000111111110000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange6)
try
{
    writeSegmentGeneric(
        "s:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, 9223372036854775807)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "1111111111111111111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange7)
try
{
    writeSegmentGeneric(
        "s:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, 9223372036854775807]");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "1111111111111111111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange8)
try
{
    writeSegmentGeneric("d_big:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, "
                        "9223372036854775807)");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "1111111111111111111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange9)
try
{
    writeSegmentGeneric("d_big:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, "
                        "9223372036854775807]");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "1111111111111111111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange10)
try
{
    writeSegmentGeneric(
        "d_big:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, "
        "9223372036854775807)",
        std::tuple{9223372036854775715, 9223372036854775807, false});
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "000001111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange11)
try
{
    writeSegmentGeneric(
        "d_big:[9223372036854775700, 9223372036854775807]:pack_size_10|d_dr:[9223372036854775754, "
        "9223372036854775807)",
        std::tuple{9223372036854775715, 9223372036854775807, true});
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "000001111111111111111111111111111111111111110000000000000000000000000000000000000"
                           "00000000000000001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_Delta)
try
{
    writeSegmentGeneric("d_mem:[0, 1):ts_1|d_mem:[0, 1):ts_2|d_mem:[0, 1):ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "001",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "010",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "100",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_Delta_Flush)
try
{
    writeSegmentGeneric("d_mem:[0, 10):shuffle:ts_1|d_mem:[3, 13):shuffle:ts_2|d_mem:[6, 16):shuffle:ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
    });


    auto guard = enableVersionChainTemporary(db_context->getGlobalContext().getSettingsRef());
    ASSERT_TRUE(dm_context->enableVersionChain());

    auto get_base_versions = [&](bool flushed) {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        auto cfs = snap->delta->getColumnFiles();
        RUNTIME_CHECK(cfs.size() == 1, cfs.size());
        if (flushed)
            RUNTIME_CHECK(cfs[0]->isTinyFile(), cfs[0]->toString());
        else
            RUNTIME_CHECK(cfs[0]->isInMemoryFile(), cfs[0]->toString());

        auto vc = createVersionChain(is_common_handle);
        return std::visit([&](auto & version_chain) { return version_chain.replaySnapshot(*dm_context, *snap); }, vc);
    };
    auto base_ver1 = get_base_versions(false);
    writeSegmentGeneric("flush_cache"); // Flush never sort data when version chain enabled.
    auto base_ver2 = get_base_versions(true);
    ASSERT_EQ(*base_ver1, *base_ver2);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_Delta_Compact)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):shuffle:ts_1|d_tiny:[3, 13):shuffle:ts_2|d_tiny:[6, 16):shuffle:ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
    });

    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFiles().size(), 3);
    }
    writeSegmentGeneric("compact_delta");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFiles().size(), 1);
    }
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_Delta_Compact_1)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):ts_1|d_tiny:[3, 13):ts_2|d_tiny:[6, 16):ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "111000000011100000001111111111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "111000000011111111110000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "111111111100000000000000000000",
    });

    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFiles().size(), 3);
    }
    writeSegmentGeneric("compact_delta");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFiles().size(), 1);
    }
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "111000000011100000001111111111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "111000000011111111110000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "111111111100000000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_DMFile_MaxVersion)
try
{
    writeSegmentGeneric(
        "d_tiny:[0, 10):shuffle:ts_1|d_tiny:[3, 13):shuffle:ts_2|d_tiny:[6, 16):shuffle:ts_3|merge_delta");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFileCount(), 0);
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "111010101001001001001010101111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "111010101010010010010101010000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "111101010100100100100000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_DMFile_MaxVersionWithDelta)
try
{
    writeSegmentGeneric("d_tiny:[0, 1):ts_1|d_tiny:[1, 2):ts_2|d_tiny:[2, 3):ts_3|d_tiny:[3, 4):ts_4|d_tiny:[4, "
                        "5):ts_5|merge_delta|d_tiny:[0, 1):ts_2");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFileCount(), 1);
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "010001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_DMFile_NotClean)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):ts_1|d_tiny:[0, 10):ts_2|d_tiny:[0, 10):ts_3|d_tiny:[0, 10):ts_4|d_tiny:[0, "
                        "10):ts_5|merge_delta:pack_size_10");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFileCount(), 0);
        const auto & dmfile = snap->stable->getDMFiles()[0];
        ASSERT_EQ(dmfile->getPacks(), 5);
        ASSERT_TRUE(std::all_of(dmfile->getPackStats().begin(), dmfile->getPackStats().end(), [](const auto & ps) {
            return ps.not_clean;
        }));
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "10000100001000010000100001000010000100001000010000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "01000010000100001000010000100001000010000100001000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "00100001000010000100001000010000100001000010000100",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 4,
        .expected_bitmap = "00010000100001000010000100001000010000100001000010",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 5,
        .expected_bitmap = "00001000010000100001000010000100001000010000100001",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, Version_DMFile_NotCleanWithDelta)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):ts_1|d_tiny:[0, 10):ts_2|d_tiny:[0, 10):ts_3|d_tiny:[0, 10):ts_4|d_tiny:[0, "
                        "10):ts_5|merge_delta:pack_size_10|d_tiny:[0, 10):ts_6");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFileCount(), 1);
        ASSERT_EQ(snap->delta->getRows(), 10);
        const auto & dmfile = snap->stable->getDMFiles()[0];
        ASSERT_EQ(dmfile->getPacks(), 5);
        ASSERT_TRUE(std::all_of(dmfile->getPackStats().begin(), dmfile->getPackStats().end(), [](const auto & ps) {
            return ps.not_clean;
        }));
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "100001000010000100001000010000100001000010000100000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "010000100001000010000100001000010000100001000010000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "001000010000100001000010000100001000010000100001000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 4,
        .expected_bitmap = "000100001000010000100001000010000100001000010000100000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 5,
        .expected_bitmap = "000010000100001000010000100001000010000100001000010000000000",
    });

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 6,
        .expected_bitmap = "000000000000000000000000000000000000000000000000001111111111",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteMark1)
try
{
    writeSegmentGeneric("d_mem:[0, 1):ts_1|d_mem_del:[0, 1):ts_2|d_mem:[0, 1):ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "001",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "100",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteMark2)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):shuffle:ts_1|d_tiny_del:[3, 13):shuffle:ts_2|d_tiny:[6, 16):shuffle:ts_3");
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteMark2_1)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):ts_1|d_tiny_del:[3, 13):ts_2|d_tiny:[6, 16):ts_3");

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "111000000000000000001111111111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "111000000000000000000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "111111111100000000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteMark3)
try
{
    writeSegmentGeneric(
        "d_tiny:[0, 10):shuffle:ts_1|d_tiny_del:[3, 13):shuffle:ts_2|d_tiny:[6, 16):shuffle:ts_3|merge_delta");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(snap->delta->getColumnFileCount(), 0);
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "111000000001001001001010101111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "111000000000000000000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "111101010100100100100000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteMark4)
try
{
    writeSegmentGeneric("d_tiny:[0, 10):shuffle:ts_1|d_tiny_del:[3, 13):shuffle:ts_2|d_tiny:[6, "
                        "16):shuffle:ts_3|merge_delta|d_mem:[2, 7):ts_4");
    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        auto cfs = snap->delta->getColumnFiles();
        ASSERT_EQ(cfs.size(), 1);
        ASSERT_TRUE(cfs[0]->isInMemoryFile()) << cfs[0]->toString();
    }

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 4,
        .expected_bitmap = "11000000000000100100101010111111111",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 3,
        .expected_bitmap = "11100000000100100100101010111100000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 2,
        .expected_bitmap = "11100000000000000000000000000000000",
    });
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .read_ts = 1,
        .expected_bitmap = "11110101010010010010000000000000000",
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, DupHandle)
try
{
    writeSegmentGeneric("d_tiny:[0, 128):ts_1|merge_delta|d_tiny:[50, 60):ts_2");
    auto [seg1, snap1] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(snap1->delta->getRows(), 10);
    ASSERT_EQ(snap1->stable->getRows(), 128);

    writeSegmentGeneric("d_tiny:[50, 60):ts_2");
    auto [seg2, snap2] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(snap2->delta->getRows(), 20);
    ASSERT_EQ(snap2->stable->getRows(), 128);

    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
    });
}
CATCH

TEST_P(SegmentBitmapFilterTest, RSFilter0)
{
    writeSegmentGeneric("s:[0, 50):pack_size_5:ts_1");
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto rs_filter_results = loadPackFilterResults(snap, {seg->getRowKeyRange()});
    ASSERT_EQ(rs_filter_results.size(), 1);
    auto & rs_filter_result = rs_filter_results[0];
    ASSERT_EQ(rs_filter_result->handle_res.size(), 10);
    ASSERT_EQ(rs_filter_result->pack_res.size(), 10);
    rs_filter_result->pack_res[4] = RSResult::None; // [20, 25)
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "11111111111111111111000001111111111111111111111111",
        .rs_filter_results = rs_filter_results,
    });
}

TEST_P(SegmentBitmapFilterTest, RSFilter1)
{
    writeSegmentGeneric("s:[0, 50):pack_size_5:ts_1|d_tiny:[22, 27):ts_2");
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto rs_filter_results = loadPackFilterResults(snap, {seg->getRowKeyRange()});
    ASSERT_EQ(rs_filter_results.size(), 1);
    auto & rs_filter_result = rs_filter_results[0];
    ASSERT_EQ(rs_filter_result->handle_res.size(), 10);
    ASSERT_EQ(rs_filter_result->pack_res.size(), 10);
    rs_filter_result->pack_res[4] = RSResult::None; // [20, 25)
    checkBitmap(CheckBitmapOptions{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_bitmap = "1111111111111111111100000001111111111111111111111111111",
        .rs_filter_results = rs_filter_results,
    });
}
} // namespace DB::DM::tests

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
    std::optional<std::tuple<Int64, Int64, bool>> rowkey_range)
{
    if (is_common_handle)
        writeSegment<String>(seg_data, rowkey_range);
    else
        writeSegment<Int64>(seg_data, rowkey_range);
}

/*
    0----------------stable_rows----------------stable_rows + delta_rows <-- append
    | stable value space | delta value space ..........................  <-- append
    |--------------------|--ColumnFilePersisted--|ColumnFileInMemory...  <-- append
    |--------------------|-Tiny|DeleteRange|Big--|ColumnFileInMemory...  <-- append

    `seg_data`: s:[a, b)|d_tiny:[a, b)|d_tiny_del:[a, b)|d_big:[a, b)|d_dr:[a, b)|d_mem:[a, b)|d_mem_del
    - s: stable
    - d_tiny: delta ColumnFileTiny
    - d_tiny_del: delta ColumnFileTiny with delete flag
    - d_big: delta ColumnFileBig
    - d_dr: delta delete range

    Returns {row_id, handle}.
    */
template <typename HandleType>
std::pair<const PaddedPODArray<UInt32> *, const std::optional<ColumnView<HandleType>>> SegmentBitmapFilterTest::
    writeSegment(std::string_view seg_data, std::optional<std::tuple<Int64, Int64, bool>> rowkey_range)
{
    if (rowkey_range)
    {
        const auto & [left, right, including_right_boundary] = *rowkey_range;
        setRowKeyRange(left, right, including_right_boundary);
    }
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
        return {nullptr, std::nullopt};
    }
    else
    {
        return {toColumnVectorDataPtr<UInt32>(hold_row_id), ColumnView<HandleType>(*hold_handle)};
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

void SegmentBitmapFilterTest::runTestCaseGeneric(
    TestCase test_case,
    int caller_line,
    const std::vector<RowID> & expected_base_versions)
{
    if (is_common_handle)
        runTestCase<String>(test_case, caller_line, expected_base_versions);
    else
        runTestCase<Int64>(test_case, caller_line, expected_base_versions);
}

template <typename HandleType>
void SegmentBitmapFilterTest::runTestCase(
    TestCase test_case,
    int caller_line,
    const std::vector<RowID> & expected_base_versions)
{
    auto info = fmt::format("caller_line={}", caller_line);
    auto [row_id, handle] = writeSegment<HandleType>(test_case.seg_data, test_case.rowkey_range);
    if (test_case.expected_size == 0)
    {
        ASSERT_EQ(nullptr, row_id) << info;
        ASSERT_EQ(std::nullopt, handle) << info;
    }
    else
    {
        ASSERT_EQ(test_case.expected_size, row_id->size()) << info;
        auto expected_row_id = genSequence<UInt32>(test_case.expected_row_id);
        ASSERT_TRUE(sequenceEqual(expected_row_id, *row_id)) << info;

        ASSERT_EQ(test_case.expected_size, handle->size()) << info;
        auto expected_handle = genHandleSequence<HandleType>(test_case.expected_handle);
        ASSERT_TRUE(sequenceEqual(expected_handle, *handle)) << info;
    }

    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = SEG_ID,
        .caller_line = caller_line,
        .expected_base_versions = expected_base_versions,
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

void SegmentBitmapFilterTest::verifyVersionChain(const VerifyVersionChainOption & opt)
{
    auto info = opt.toDebugString();
    auto [seg, snap] = getSegmentForRead(opt.seg_id);

    if (opt.expected_base_versions)
    {
        const auto & expected_base_versions = *(opt.expected_base_versions);
        auto actual_base_versions = std::visit(
            [&](auto & version_chain) { return version_chain.replaySnapshot(*dm_context, *snap); },
            seg->version_chain);
        ASSERT_EQ(expected_base_versions.size(), actual_base_versions->size()) << info;
        for (size_t i = 0; i < expected_base_versions.size(); ++i)
            ASSERT_EQ(expected_base_versions[i], (*actual_base_versions)[i]) << fmt::format("i={}, {}", i, info);
    }
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

void SegmentBitmapFilterTest::verifyNotCleanAndDeleted(const verifyNotCleanAndDeletedOption & opt)
{
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    const auto & dmfile = snap->stable->getDMFiles()[0];
    ASSERT_GT(dmfile->getPacks(), opt.pack_id) << opt.caller_line;
    const auto & pack_stat = dmfile->getPackStats()[opt.pack_id];
    const auto & pack_property = dmfile->getPackProperties().property()[opt.pack_id];
    ASSERT_EQ(pack_stat.rows, opt.expected_pack_rows) << opt.caller_line;
    ASSERT_EQ(pack_stat.not_clean, opt.expected_not_clean_rows) << opt.caller_line;
    ASSERT_EQ(pack_property.deleted_rows(), opt.expected_deleted_rows) << opt.caller_line;
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
        __LINE__,
        std::vector<RowID>(1000, NotExistRowID));
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory2)
try
{
    std::vector<RowID> excepted_base_versions(2000);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 0); // d_mem:[0, 1000)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[0, 1000)",
            .expected_size = 1000,
            .expected_row_id = "[1000, 2000)",
            .expected_handle = "[0, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory3)
try
{
    std::vector<RowID> excepted_base_versions(1100);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 100); // d_mem:[100, 200)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[100, 200)",
            .expected_size = 1000,
            .expected_row_id = "[0, 100)|[1000, 1100)|[200, 1000)",
            .expected_handle = "[0, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory4)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(
        excepted_base_versions.begin(),
        excepted_base_versions.begin() + 1100,
        NotExistRowID); // d_mem:[0, 1000) + d_mem:[-100, 0)
    std::iota(excepted_base_versions.begin() + 1100, excepted_base_versions.end(), 0); // d_mem:[0, 100)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem:[-100, 100)",
            .expected_size = 1100,
            .expected_row_id = "[1000, 1200)|[100, 1000)",
            .expected_handle = "[-100, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory5)
try
{
    std::vector<RowID> excepted_base_versions(2000);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 0); // d_mem_del:[0, 1000)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[0, 1000)",
            .expected_size = 0,
            .expected_row_id = "",
            .expected_handle = ""},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory6)
try
{
    std::vector<RowID> excepted_base_versions(1100);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 100); // d_mem_del:[100, 200)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[100, 200)",
            .expected_size = 900,
            .expected_row_id = "[0, 100)|[200, 1000)",
            .expected_handle = "[0, 100)|[200, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory7)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(
        excepted_base_versions.begin(),
        excepted_base_versions.begin() + 1100,
        NotExistRowID); // d_mem:[0, 1000) + d_mem:[-100, 0)
    std::iota(excepted_base_versions.begin() + 1100, excepted_base_versions.end(), 0); // d_mem_del:[0, 100)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_mem:[0, 1000)|d_mem_del:[-100, 100)",
            .expected_size = 900,
            .expected_row_id = "[100, 1000)",
            .expected_handle = "[100, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Tiny1)
try
{
    std::vector<RowID> excepted_base_versions(1200);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 300,
        100); // d_mem:[200, 500)
    std::fill(
        excepted_base_versions.begin() + 400 + 300,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[500, 1000)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_mem:[200, 1000)",
            .expected_size = 900,
            .expected_row_id = "[0, 100)|[400, 1200)",
            .expected_handle = "[100, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, TinyDel1)
try
{
    std::vector<RowID> excepted_base_versions(600);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 100,
        100); // d_tiny_del:[200, 300)
    std::fill(
        excepted_base_versions.begin() + 400 + 100,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[0, 100)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)",
            .expected_size = 400,
            .expected_row_id = "[500, 600)|[0, 100)|[200, 400)",
            .expected_handle = "[0, 200)|[300, 500)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, DeleteRange)
try
{
    std::vector<RowID> excepted_base_versions(450);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(excepted_base_versions.begin() + 400, excepted_base_versions.begin() + 400 + 10, 140); // d_mem:[240, 250)
    std::fill(
        excepted_base_versions.begin() + 400 + 10,
        excepted_base_versions.end(),
        NotExistRowID); // d_mem:[250, 290)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)",
            .expected_size = 390,
            .expected_row_id = "[0, 140)|[400, 450)|[200, 400)",
            .expected_handle = "[100, 290)|[300, 500)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Big)
try
{
    std::vector<RowID> excepted_base_versions(400 + 750 + 50);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 400, NotExistRowID); // d_tiny:[100, 500)
    std::iota(
        excepted_base_versions.begin() + 400,
        excepted_base_versions.begin() + 400 + 250,
        150); // d_big:[250, 500)
    std::fill(
        excepted_base_versions.begin() + 400 + 250,
        excepted_base_versions.begin() + 400 + 250 + 500,
        NotExistRowID); // d_big:[500, 1000)
    std::iota(excepted_base_versions.begin() + 400 + 250 + 500, excepted_base_versions.end(), 140); // d_mem:[240, 290)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)",
            .expected_size = 900,
            .expected_row_id = "[0, 140)|[1150, 1200)|[440, 1150)",
            .expected_handle = "[100, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable1)
try
{
    std::vector<RowID> excepted_base_versions{};
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)",
            .expected_size = 1024,
            .expected_row_id = "[0, 1024)",
            .expected_handle = "[0, 1024)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable2)
try
{
    std::vector<RowID> excepted_base_versions{};
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[0, 1023)",
            .expected_size = 1,
            .expected_row_id = "[1023, 1024)",
            .expected_handle = "[1023, 1024)"},
        __LINE__,
        excepted_base_versions);
}
CATCH


TEST_P(SegmentBitmapFilterTest, Stable3)
try
{
    std::vector<RowID> excepted_base_versions(10);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.end(), 300); // s:[300, 310)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)",
            .expected_size = 886,
            .expected_row_id = "[0, 128)|[256, 300)|[310, 1024)",
            .expected_handle = "[0, 128)|[256, 300)|[310, 1024)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Mix)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 946,
            .expected_row_id = "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            .expected_handle = "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Ranges)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    read_ranges.emplace_back(buildRowKeyRange(222, 244, is_common_handle));
    read_ranges.emplace_back(buildRowKeyRange(300, 303, is_common_handle));
    read_ranges.emplace_back(buildRowKeyRange(555, 666, is_common_handle));
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 136,
            .expected_row_id = "[1056, 1078)|[1091, 1094)|[555, 666)",
            .expected_handle = "[222, 244)|[300, 303)|[555, 666)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, LogicalSplit)
try
{
    std::vector<RowID> excepted_base_versions(10 + 55 + 7);
    std::iota(excepted_base_versions.begin(), excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::fill(
        excepted_base_versions.begin() + 10,
        excepted_base_versions.begin() + 10 + 55,
        NotExistRowID); // d_tiny:[200, 255)
    std::iota(excepted_base_versions.begin() + 10 + 55, excepted_base_versions.end(), 298); // d_mem:[298, 305)
    runTestCaseGeneric(
        TestCase{
            .seg_data = "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            .expected_size = 946,
            .expected_row_id = "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            .expected_handle = "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__,
        excepted_base_versions);

    auto new_seg_id = splitSegmentAt(SEG_ID, 512, Segment::SplitMode::Logical);

    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));
    // segment_range: [-inf, 512)
    // "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)"
    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_base_versions = excepted_base_versions,
    });
    // segment_range: [512, +inf)
    // "s:[0, 1024)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)"
    std::vector<RowID> other_excepted_base_versions(10 + 55 + 7);
    std::iota(other_excepted_base_versions.begin(), other_excepted_base_versions.begin() + 10, 300); // s:[300, 310)
    std::iota(
        other_excepted_base_versions.begin() + 10,
        other_excepted_base_versions.begin() + 10 + 55,
        200); // d_tiny:[200, 255)
    std::iota(
        other_excepted_base_versions.begin() + 10 + 55,
        other_excepted_base_versions.end(),
        298); // d_mem:[298, 305)
    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = *new_seg_id,
        .caller_line = __LINE__,
        .expected_base_versions = other_excepted_base_versions,
    });

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
    auto bitmap_filter = seg->buildBitmapFilterStableOnly(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        loadPackFilterResults(snap, {seg->getRowKeyRange()}),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_NE(bitmap_filter, nullptr);
    std::string expect_result;
    expect_result.append(std::string(25000, '1'));
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_base_versions = std::vector<RowID>{}});
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
        auto bitmap_filter = seg->buildBitmapFilterStableOnly(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            loadPackFilterResults(snap, {seg->getRowKeyRange()}),
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
        verifyVersionChain(VerifyVersionChainOption{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .expected_base_versions = std::vector<RowID>{}});
    }
    {
        // Stale read
        ASSERT_EQ(version, 2);
        auto bitmap_filter = seg->buildBitmapFilterStableOnly(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            loadPackFilterResults(snap, {seg->getRowKeyRange()}),
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

        verifyVersionChain(VerifyVersionChainOption{
            .seg_id = SEG_ID,
            .caller_line = __LINE__,
            .read_ts = 1,
            .expected_base_versions = std::vector<RowID>{},
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

    auto ranges = std::vector<RowKeyRange>{buildRowKeyRange(10000, 50000, is_common_handle)}; // [10000, 50000)
    auto bitmap_filter = seg->buildBitmapFilterStableOnly(
        *dm_context,
        snap,
        ranges,
        loadPackFilterResults(snap, ranges),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_NE(bitmap_filter, nullptr);
    std::string expect_result;
    // [0, 10000) is filtered by range.
    expect_result.append(std::string(10000, '0'));
    expect_result.append(std::string(40000, '1'));
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);

    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_base_versions = std::vector<RowID>{}});
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


    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = SEG_ID,
        .caller_line = __LINE__,
        .expected_base_versions = std::vector<RowID>{}});
    verifyVersionChain(VerifyVersionChainOption{
        .seg_id = *new_seg_id,
        .caller_line = __LINE__,
        .expected_base_versions = std::vector<RowID>{}});

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
            .rowkey_range = std::tuple<Int64, Int64, bool>{275, 295, false}},
        __LINE__,
        std::vector<RowID>(30, NotExistRowID));

    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        loadPackFilterResults(snap, {seg->getRowKeyRange()}),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(bitmap_filter->size(), 30);
    ASSERT_EQ(bitmap_filter->count(), 20); // `count()` returns the number of bit has been set.
    ASSERT_EQ(bitmap_filter->toDebugString(), "000001111111111111111111100000");
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
        __LINE__,
        {});

    {
        auto [seg, snap] = getSegmentForRead(SEG_ID);
        ASSERT_EQ(seg->stable->getDMFilesPacks(), 75);
    }

    // For Stable, all packs of DMFile will be considered in BitmapFilter.
    setRowKeyRange(275, 295, false); // Shrinking range
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        loadPackFilterResults(snap, {seg->getRowKeyRange()}),
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

        auto bitmap_filter
            = seg->buildBitmapFilterStableOnly(*dm_context, snap, ranges, pack_filter_results, 2, DEFAULT_BLOCK_SIZE);

        ASSERT_EQ(expect_result, bitmap_filter->toDebugString());

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

        auto bitmap_filter = seg->buildBitmapFilterNormal(
            *dm_context,
            snap,
            ranges,
            pack_filter_results,
            start_ts,
            DEFAULT_BLOCK_SIZE);

        ASSERT_EQ(expect_result, bitmap_filter->toDebugString());

        deleteRangeSegment(SEG_ID);
    }
}

} // namespace DB::DM::tests

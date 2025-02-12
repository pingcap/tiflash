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
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/VersionChain/BuildBitmapFilter.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>

using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{

class SegmentBitmapFilterTest
    : public SegmentTestBasic
    , public testing::WithParamInterface</*is_common_handle*/ bool>
{
public:
    void SetUp() override
    {
        is_common_handle = GetParam();
        SegmentTestBasic::SetUp(SegmentTestOptions{.is_common_handle = is_common_handle});
    }

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
        itr->second->rowkey_range = buildRowKeyRange(begin, end, is_common_handle);
    }

    void writeSegmentGeneric(std::string_view seg_data)
    {
        if (is_common_handle)
            writeSegment<String>(seg_data);
        else
            writeSegment<Int64>(seg_data);
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
    template <typename HandleType>
    std::pair<const PaddedPODArray<UInt32> *, const std::optional<ColumnView<HandleType>>> writeSegment(
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
            return {nullptr, std::nullopt};
        }
        else
        {
            return {toColumnVectorDataPtr<UInt32>(hold_row_id), ColumnView<HandleType>(*hold_handle)};
        }
    }

    void writeSegment(const SegDataUnit & unit)
    {
        const auto & type = unit.type;
        auto [begin, end, including_right_boundary] = unit.range;
        const auto write_count = end - begin + including_right_boundary;
        if (type == "d_mem")
        {
            SegmentTestBasic::writeSegment(SEG_ID, write_count, begin);
        }
        else if (type == "d_mem_del")
        {
            SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, write_count, begin);
        }
        else if (type == "d_tiny")
        {
            SegmentTestBasic::writeSegment(SEG_ID, write_count, begin);
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
            SegmentTestBasic::writeSegmentWithDeleteRange(SEG_ID, begin, end, is_common_handle);
        }
        else if (type == "s")
        {
            SegmentTestBasic::writeSegment(SEG_ID, write_count, begin);
            if (unit.pack_size)
            {
                db_context->getSettingsRef().dt_segment_stable_pack_rows = *(unit.pack_size);
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

    inline static constexpr bool use_version_chain = true;
    inline static constexpr auto max_read_ts = std::numeric_limits<UInt64>::max();

    void runTestCaseGeneric(TestCase test_case, int caller_line, const std::vector<RowID> & expected_base_versions)
    {
        if (is_common_handle)
            runTestCase<String>(test_case, caller_line, expected_base_versions);
        else
            runTestCase<Int64>(test_case, caller_line, expected_base_versions);
    }

    template <typename HandleType>
    void runTestCase(TestCase test_case, int caller_line, const std::vector<RowID> & expected_base_versions)
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

        verifyVersionChain(SEG_ID, caller_line, expected_base_versions);
    }

    auto loadPackFilterResults(const SegmentSnapshotPtr & snap, const RowKeyRanges & ranges)
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

    void verifyVersionChain(
        const PageIdU64 seg_id,
        int caller_line,
        const std::vector<RowID> & excepted_base_versions,
        const UInt64 read_ts = max_read_ts)
    {
        auto info = fmt::format("caller_line={}", caller_line);
        auto [seg, snap] = getSegmentForRead(seg_id);
        auto actual_base_versions = std::visit(
            [&](auto & version_chain) { return version_chain.replaySnapshot(*dm_context, *snap); },
            seg->version_chain);
        ASSERT_EQ(excepted_base_versions.size(), actual_base_versions->size()) << info;
        for (size_t i = 0; i < excepted_base_versions.size(); ++i)
            ASSERT_EQ(excepted_base_versions[i], (*actual_base_versions)[i]) << fmt::format("i={}, {}", i, info);

        auto bitmap_filter_version_chain = seg->buildBitmapFilter(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            loadPackFilterResults(snap, {seg->getRowKeyRange()}),
            read_ts,
            DEFAULT_BLOCK_SIZE,
            use_version_chain);

        auto bitmap_filter_delta_index = seg->buildBitmapFilter(
            *dm_context,
            snap,
            {seg->getRowKeyRange()},
            loadPackFilterResults(snap, {seg->getRowKeyRange()}),
            read_ts,
            DEFAULT_BLOCK_SIZE,
            !use_version_chain);

        ASSERT_EQ(bitmap_filter_delta_index->toDebugString(), bitmap_filter_version_chain->toDebugString()) << info;
    }

    void checkHandle(PageIdU64 seg_id, std::string_view seq_ranges, int caller_line)
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

    bool is_common_handle = false;
};

INSTANTIATE_TEST_CASE_P(MVCC, SegmentBitmapFilterTest, /* is_common_handle */ ::testing::Bool());

TEST_P(SegmentBitmapFilterTest, InMemory1)
try
{
    runTestCaseGeneric(
        TestCase{"d_mem:[0, 1000)", 1000, "[0, 1000)", "[0, 1000)"},
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
        TestCase{"d_mem:[0, 1000)|d_mem:[0, 1000)", 1000, "[1000, 2000)", "[0, 1000)"},
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
        TestCase{"d_mem:[0, 1000)|d_mem:[100, 200)", 1000, "[0, 100)|[1000, 1100)|[200, 1000)", "[0, 1000)"},
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
        TestCase{"d_mem:[0, 1000)|d_mem:[-100, 100)", 1100, "[1000, 1200)|[100, 1000)", "[-100, 1000)"},
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
    runTestCaseGeneric(TestCase{"d_mem:[0, 1000)|d_mem_del:[0, 1000)", 0, "", ""}, __LINE__, excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, InMemory6)
try
{
    std::vector<RowID> excepted_base_versions(1100);
    std::fill(excepted_base_versions.begin(), excepted_base_versions.begin() + 1000, NotExistRowID); // d_mem:[0, 1000)
    std::iota(excepted_base_versions.begin() + 1000, excepted_base_versions.end(), 100); // d_mem_del:[100, 200)
    runTestCaseGeneric(
        TestCase{"d_mem:[0, 1000)|d_mem_del:[100, 200)", 900, "[0, 100)|[200, 1000)", "[0, 100)|[200, 1000)"},
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
        TestCase{"d_mem:[0, 1000)|d_mem_del:[-100, 100)", 900, "[100, 1000)", "[100, 1000)"},
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
        TestCase{"d_tiny:[100, 500)|d_mem:[200, 1000)", 900, "[0, 100)|[400, 1200)", "[100, 1000)"},
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
            "d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)",
            400,
            "[500, 600)|[0, 100)|[200, 400)",
            "[0, 200)|[300, 500)"},
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
            "d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)",
            390,
            "[0, 140)|[400, 450)|[200, 400)",
            "[100, 290)|[300, 500)"},
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
            "d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)",
            900,
            "[0, 140)|[1150, 1200)|[440, 1150)",
            "[100, 1000)"},
        __LINE__,
        excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable1)
try
{
    std::vector<RowID> excepted_base_versions{};
    runTestCaseGeneric(TestCase{"s:[0, 1024)", 1024, "[0, 1024)", "[0, 1024)"}, __LINE__, excepted_base_versions);
}
CATCH

TEST_P(SegmentBitmapFilterTest, Stable2)
try
{
    std::vector<RowID> excepted_base_versions{};
    runTestCaseGeneric(
        TestCase{"s:[0, 1024)|d_dr:[0, 1023)", 1, "[1023, 1024)", "[1023, 1024)"},
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
            "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)",
            886,
            "[0, 128)|[256, 300)|[310, 1024)",
            "[0, 128)|[256, 300)|[310, 1024)"},
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
            "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            946,
            "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
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
            "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            136,
            "[1056, 1078)|[1091, 1094)|[555, 666)",
            "[222, 244)|[300, 303)|[555, 666)"},
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
            "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
            946,
            "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
            "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"},
        __LINE__,
        excepted_base_versions);

    auto new_seg_id = splitSegmentAt(SEG_ID, 512, Segment::SplitMode::Logical);

    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({SEG_ID, *new_seg_id}));
    // segment_range: [-inf, 512)
    // "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)"
    verifyVersionChain(SEG_ID, __LINE__, excepted_base_versions);
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
    verifyVersionChain(*new_seg_id, __LINE__, other_excepted_base_versions);

    checkHandle(SEG_ID, "[0, 128)|[200, 255)|[256, 305)|[310, 512)", __LINE__);

    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto & left_r = toColumnVectorData<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 512)");
    ASSERT_EQ(expected_left_row_id.size(), left_r.size());
    ASSERT_TRUE(sequenceEqual(expected_left_row_id, left_r));

    checkHandle(*new_seg_id, "[512, 1024)", __LINE__);

    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto & right_r = toColumnVectorData<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[512, 1024)");
    ASSERT_EQ(expected_right_row_id.size(), right_r.size());
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
    verifyVersionChain(SEG_ID, __LINE__, {});
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
        verifyVersionChain(SEG_ID, __LINE__, {});
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
        verifyVersionChain(SEG_ID, __LINE__, {}, 1);
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

    verifyVersionChain(SEG_ID, __LINE__, {});
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

    verifyVersionChain(SEG_ID, __LINE__, {});
    verifyVersionChain(*new_seg_id, __LINE__, {});

    checkHandle(SEG_ID, "[0, 25000)", __LINE__);

    auto left_row_id = getSegmentRowId(SEG_ID, {});
    const auto & left_r = toColumnVectorData<UInt32>(left_row_id);
    auto expected_left_row_id = genSequence<UInt32>("[0, 25000)");
    ASSERT_EQ(expected_left_row_id.size(), left_r.size());
    ASSERT_TRUE(sequenceEqual(expected_left_row_id, left_r));

    checkHandle(*new_seg_id, "[25000, 50000)", __LINE__);

    auto right_row_id = getSegmentRowId(*new_seg_id, {});
    const auto & right_r = toColumnVectorData<UInt32>(right_row_id);
    auto expected_right_row_id = genSequence<UInt32>("[25000, 50000)");
    ASSERT_EQ(expected_right_row_id.size(), right_r.size());
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
            /*seg_data*/ "d_big:[250, 1000):10",
            /*expected_size*/ 20,
            /*expected_row_id*/ "[5, 25)",
            /*expected_handle*/ "[275, 295)",
            /*rowkey_range*/ std::pair<Int64, Int64>{275, 295}},
        __LINE__,
        std::vector<RowID>(30, NotExistRowID));

    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        loadPackFilterResults(snap, {seg->getRowKeyRange()}),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        use_version_chain);
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
            /*seg_data*/ "s:[250, 1000):10",
            /*expected_size*/ 750,
            /*expected_row_id*/ "[0, 750)",
            /*expected_handle*/ "[250, 1000)"},
        __LINE__,
        {});

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
        loadPackFilterResults(snap, {seg->getRowKeyRange()}),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        use_version_chain);
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

// Since rowkey_range is a left closed and right open interval, the right boundary is not included.
// So the maximum value of Int64 needs to be handled carefully.
TEST_P(SegmentBitmapFilterTest, RowKeyFilter_Int64Boundary)
try
{
    if (is_common_handle)
        return;

    runTestCaseGeneric(
        TestCase{
            "d_mem:[-9223372036854775808, -9223372036854775800)|d_mem:[9223372036854775800, 9223372036854775807]",
            16,
            "[0, 16)",
            "[-9223372036854775808, -9223372036854775800)|[9223372036854775800, 9223372036854775807]"},
        __LINE__,
        std::vector<RowID>(16, NotExistRowID));

    auto [seg, snap] = getSegmentForRead(SEG_ID);

    {
        RowKeyRanges ranges = {RowKeyRange{
            RowKeyValue::fromHandle(std::numeric_limits<Int64>::min()),
            RowKeyValue::fromHandle(std::numeric_limits<Int64>::max()),
            is_common_handle,
            1}};
        ASSERT_TRUE(ranges[0].isStartInfinite());
        ASSERT_FALSE(ranges[0].isEndInfinite());

        //RowKeyRanges ranges = {RowKeyRange::newAll(is_common_handle, 1)};
        auto bitmap_filter = seg->buildBitmapFilter(
            *dm_context,
            snap,
            ranges,
            loadPackFilterResults(snap, ranges),
            max_read_ts,
            DEFAULT_BLOCK_SIZE,
            use_version_chain);

        ASSERT_EQ(bitmap_filter->toDebugString(), "1111111111111110");
    }

    {
        RowKeyRanges ranges = {RowKeyRange::newAll(is_common_handle, 1)};
        ASSERT_TRUE(ranges[0].isStartInfinite());
        ASSERT_TRUE(ranges[0].isEndInfinite());
        auto bitmap_filter = seg->buildBitmapFilter(
            *dm_context,
            snap,
            ranges,
            loadPackFilterResults(snap, ranges),
            max_read_ts,
            DEFAULT_BLOCK_SIZE,
            use_version_chain);

        ASSERT_EQ(bitmap_filter->toDebugString(), "1111111111111111");
    }
}
CATCH

TEST_P(SegmentBitmapFilterTest, RowKeyFilter_Stable)
try
{
    runTestCaseGeneric(
        TestCase{
            /*seg_data*/ "s:[250, 1000):50",
            /*expected_size*/ 750,
            /*expected_row_id*/ "[0, 750)",
            /*expected_handle*/ "[250, 1000)"},
        __LINE__,
        {});

    RowKeyRanges read_ranges = {
        buildRowKeyRange(318, 520, is_common_handle),
        buildRowKeyRange(618, 737, is_common_handle),
        buildRowKeyRange(918, 998, is_common_handle),
    };
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        loadPackFilterResults(snap, read_ranges),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        use_version_chain);

    String expect_result(750, '0');
    std::fill(expect_result.begin() + 318 - 250, expect_result.begin() + 520 - 250, '1');
    std::fill(expect_result.begin() + 618 - 250, expect_result.begin() + 737 - 250, '1');
    std::fill(expect_result.begin() + 918 - 250, expect_result.begin() + 998 - 250, '1');
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
}
CATCH

TEST_P(SegmentBitmapFilterTest, RowKeyFilter_CFBig)
try
{
    runTestCaseGeneric(
        TestCase{
            /*seg_data*/ "d_big:[250, 1000):50",
            /*expected_size*/ 500,
            /*expected_row_id*/ "[38, 538)",
            /*expected_handle*/ "[388, 888)",
            /*segment_range*/ std::pair{388, 888}},
        __LINE__,
        std::vector<RowID>(550, NotExistRowID));

    RowKeyRanges read_ranges = {
        buildRowKeyRange(318, 520, is_common_handle),
        buildRowKeyRange(618, 737, is_common_handle),
        buildRowKeyRange(818, 998, is_common_handle),
    };
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    read_ranges = Segment::shrinkRowKeyRanges(seg->rowkey_range, read_ranges);
    auto bitmap_filter = seg->buildBitmapFilter(
        *dm_context,
        snap,
        read_ranges,
        loadPackFilterResults(snap, read_ranges),
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        use_version_chain);

    String expect_result(550, '0');
    std::fill(expect_result.begin() + 388 - 350, expect_result.begin() + 520 - 350, '1');
    std::fill(expect_result.begin() + 618 - 350, expect_result.begin() + 737 - 350, '1');
    std::fill(expect_result.begin() + 818 - 350, expect_result.begin() + 888 - 350, '1');
    ASSERT_EQ(bitmap_filter->toDebugString(), expect_result);
}
CATCH

} // namespace DB::DM::tests

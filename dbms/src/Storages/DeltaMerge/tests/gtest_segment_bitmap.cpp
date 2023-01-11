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
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SyncPoint/Ctl.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
#include <gtest/gtest.h>

#include <boost/algorithm/string.hpp>

using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{
template <typename E, typename A>
::testing::AssertionResult sequenceEqual(const E * expected, const A * actual, size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        if (expected[i] != actual[i])
        {
            return ::testing::AssertionFailure()
                << fmt::format("Value at index {} mismatch: expected {} vs actual {}. expected => {} actual => {}",
                               i,
                               expected[i],
                               actual[i],
                               std::vector<E>(expected, expected + size),
                               std::vector<A>(actual, actual + size));
        }
    }
    return ::testing::AssertionSuccess();
}

template <typename T>
std::vector<T> genSequence(T begin, T end)
{
    auto size = end - begin;
    std::vector<T> v(size);
    std::iota(v.begin(), v.end(), begin);
    return v;
}

template <typename T>
std::vector<T> genSequence(const std::vector<std::pair<T, T>> & ranges)
{
    std::vector<T> res;
    for (auto [begin, end] : ranges)
    {
        auto v = genSequence(begin, end);
        res.insert(res.end(), v.begin(), v.end());
    }
    return res;
}

// "[a, b)" => std::pair{a, b}
template <typename T>
std::pair<T, T> parseRange(String & str_range)
{
    boost::algorithm::trim(str_range);
    RUNTIME_CHECK(str_range.front() == '[' && str_range.back() == ')', str_range);
    std::vector<String> values;
    str_range = str_range.substr(1, str_range.size() - 2);
    boost::split(values, str_range, boost::is_any_of(","));
    RUNTIME_CHECK(values.size() == 2, str_range);
    return {static_cast<T>(std::stol(values[0])), static_cast<T>(std::stol(values[1]))};
}

// "[a, b)|[c, d)" => [std::pair{a, b}, std::pair{c, d}]
template <typename T>
std::vector<std::pair<T, T>> parseRanges(std::string_view str_ranges)
{
    std::vector<String> ranges;
    boost::split(ranges, str_ranges, boost::is_any_of("|"));
    RUNTIME_CHECK(!ranges.empty(), str_ranges);
    std::vector<std::pair<T, T>> vector_ranges;
    for (auto & r : ranges)
    {
        vector_ranges.emplace_back(parseRange<T>(r));
    }
    return vector_ranges;
}

template <typename T>
std::vector<T> genSequence(std::string_view str_ranges)
{
    auto vector_ranges = parseRanges<T>(str_ranges);
    return genSequence(vector_ranges);
}

struct SegDataUnit
{
    String type;
    std::pair<Int64, Int64> range;
};

// "type:[a, b)" => SegDataUnit
SegDataUnit parseSegDataUnit(String & s)
{
    boost::algorithm::trim(s);
    std::vector<String> values;
    boost::split(values, s, boost::is_any_of(":"));
    RUNTIME_CHECK(values.size() == 2, s);
    return SegDataUnit{boost::algorithm::trim_copy(values[0]), parseRange<Int64>(values[1])};
}

void check(const std::vector<SegDataUnit> & seg_data_units)
{
    RUNTIME_CHECK(!seg_data_units.empty());
    std::vector<size_t> stable_units;
    std::vector<size_t> mem_units;
    for (size_t i = 0; i < seg_data_units.size(); i++)
    {
        const auto & type = seg_data_units[i].type;
        if (type == "s")
        {
            stable_units.emplace_back(i);
        }
        else if (type == "d_mem" || type == "d_mem_del")
        {
            mem_units.emplace_back(i);
        }
        auto [begin, end] = seg_data_units[i].range;
        RUNTIME_CHECK(begin < end, begin, end);
    }
    RUNTIME_CHECK(stable_units.empty() || (stable_units.size() == 1 && stable_units[0] == 0));
    std::vector<size_t> expected_mem_units(mem_units.size());
    std::iota(expected_mem_units.begin(), expected_mem_units.end(), seg_data_units.size() - mem_units.size());
    RUNTIME_CHECK(mem_units == expected_mem_units, expected_mem_units, mem_units);
}

std::vector<SegDataUnit> parseSegData(std::string_view seg_data)
{
    std::vector<String> str_seg_data_units;
    boost::split(str_seg_data_units, seg_data, boost::is_any_of("|"));
    RUNTIME_CHECK(!str_seg_data_units.empty(), seg_data);
    std::vector<SegDataUnit> seg_data_units;
    for (auto & s : str_seg_data_units)
    {
        seg_data_units.emplace_back(parseSegDataUnit(s));
    }
    check(seg_data_units);
    return seg_data_units;
}

class SegmentBitmapFilterTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentBitmapFilterTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    ColumnPtr hold_row_id;
    ColumnPtr hold_handle;
    RowKeyRanges read_ranges;

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
    std::pair<const PaddedPODArray<UInt32> *, const PaddedPODArray<Int64> *> writeSegment(std::string_view seg_data)
    {
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
            SegmentTestBasic::ingestDTFileIntoDelta(SEG_ID, end - begin, begin, false);
        }
        else if (type == "d_dr")
        {
            SegmentTestBasic::writeSegmentWithDeleteRange(SEG_ID, begin, end);
        }
        else if (type == "s")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
            SegmentTestBasic::mergeSegmentDelta(SEG_ID);
        }
        else
        {
            RUNTIME_CHECK(false, type);
        }
    }

    struct TestCase
    {
        TestCase(std::string_view seg_data_,
                 size_t expected_size_,
                 std::string_view expected_row_id_,
                 std::string_view expected_handle_)
            : seg_data(seg_data_)
            , expected_size(expected_size_)
            , expected_row_id(expected_row_id_)
            , expected_handle(expected_handle_)
        {}
        std::string seg_data;
        size_t expected_size;
        std::string expected_row_id;
        std::string expected_handle;
    };

    void runTestCase(TestCase test_case)
    {
        auto [row_id, handle] = writeSegment(test_case.seg_data);
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

TEST_F(SegmentBitmapFilterTest, InMemory_1)
try
{
    runTestCase(TestCase(
        "d_mem:[0, 1000)",
        1000,
        "[0, 1000)",
        "[0, 1000)"));
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_2)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem:[0, 1000)",
        1000,
        "[1000, 2000)",
        "[0, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_3)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem:[100, 200)",
        1000,
        "[0, 100)|[1000, 1100)|[200, 1000)",
        "[0, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_4)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem:[-100, 100)",
        1100,
        "[1000, 1200)|[100, 1000)",
        "[-100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_5)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem_del:[0, 1000)",
        0,
        "",
        ""});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_6)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem_del:[100, 200)",
        900,
        "[0, 100)|[200, 1000)",
        "[0, 100)|[200, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_7)
try
{
    runTestCase(TestCase{
        "d_mem:[0, 1000)|d_mem_del:[-100, 100)",
        900,
        "[100, 1000)",
        "[100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Tiny_1)
try
{
    runTestCase(TestCase{
        "d_tiny:[100, 500)|d_mem:[200, 1000)",
        900,
        "[0, 100)|[400, 1200)",
        "[100, 1000)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, TinyDel_1)
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

TEST_F(SegmentBitmapFilterTest, Stable_1)
try
{
    runTestCase(TestCase{
        "s:[0, 1024)",
        1024,
        "[0, 1024)",
        "[0, 1024)"});
}
CATCH

TEST_F(SegmentBitmapFilterTest, Stable_2)
try
{
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[0, 1023)",
        1,
        "[1023, 1024)",
        "[1023, 1024)"});
}
CATCH


TEST_F(SegmentBitmapFilterTest, Stable_3)
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
    runTestCase(TestCase{
        "s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)",
        946,
        "[0, 128)|[1034, 1089)|[256, 298)|[1089, 1096)|[310, 1024)",
        "[0, 128)|[200, 255)|[256, 305)|[310, 1024)"});
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
    writeSegment("d_mem:[0, 10000)|d_mem:[20000, 25000)");
    mergeSegmentDelta(SEG_ID, true);
    auto [seg, snap] = getSegmentForRead(SEG_ID);
    ASSERT_EQ(seg->getDelta()->getRows(), 0);
    ASSERT_EQ(seg->getDelta()->getDeletes(), 0);
    ASSERT_EQ(seg->getStable()->getRows(), 15000);
    auto bitmap_filter = seg->buildBitmapFilterStableOnly(
        *dm_context,
        snap,
        {seg->getRowKeyRange()},
        EMPTY_FILTER,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE);
    ASSERT_NE(bitmap_filter, nullptr);
    std::string expect_result;
    expect_result.append(std::string(15000, '1'));
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
            EMPTY_FILTER,
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
            EMPTY_FILTER,
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

} // namespace DB::DM::tests

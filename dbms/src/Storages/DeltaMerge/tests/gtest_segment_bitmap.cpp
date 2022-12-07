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
::testing::AssertionResult sequenceEqual(const E * except, const A * actual, size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        if (except[i] != actual[i])
        {
            return ::testing::AssertionFailure()
                << fmt::format("Value at index {} mismatch: except {} vs actual {}. except => {} actual => {}",
                               i,
                               except[i],
                               actual[i],
                               std::vector<E>(except, except + size),
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
    std::vector<size_t> except_mem_units(mem_units.size());
    std::iota(except_mem_units.begin(), except_mem_units.end(), seg_data_units.size() - mem_units.size());
    RUNTIME_CHECK(mem_units == except_mem_units, except_mem_units, mem_units);
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
    /*
    s:[a, b)|d_tiny:[a, b)|d_tiny_del:[a, b)|d_big:[a, b)|d_dr:[a, b)|d_mem:[a, b)|d_mem_del
    - s: stable
    - d_tiny: delta ColumnFileTiny
    - d_del_tiny: delta ColumnFileTiny with delete flag
    - d_big: delta ColumnFileBig
    - d_dr: delta delete range
    */
    std::pair<const PaddedPODArray<UInt32> *, const PaddedPODArray<Int64> *> writeSegment(std::string_view seg_data)
    {
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }
        hold_row_id = getSegmentRowId(SEG_ID);
        hold_handle = getSegmentHandle(SEG_ID);
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
        else
        {
            RUNTIME_CHECK(false, type);
        }
    }
};

/*
0----------------stable_rows----------------stable_rows + delta_rows
| stable value space | delta value space .......................... <-- append
|--------------------|--ColumnFilePersisted--|ColumnFileInMemory... <-- append
|--------------------|-Tiny|DeleteRange|Big--|ColumnFileInMemory... <-- append
*/


/*
     |stable|Persisted|InMemory
data |empty |empty    |[0, 1000)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_1)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)");
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[0, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1000));
    }
}
CATCH

/*
     |stable|Persisted|InMemory
data |empty |empty    |[0, 1000),[0, 1000)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_2)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem:[0, 1000)");
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[1000, 2000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1000));
    }
}
CATCH

/*
     |stable|Persisted|InMemory
data |empty |empty    |[0, 1000),[100, 200)
id   |empty |empty    |[0, 1000),[1000, 1100)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_3)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem:[100, 200)");
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[0, 100)|[1000, 1100)|[200, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1000));
    }
}
CATCH

/*
     |stable|Persisted|InMemory
data |empty |empty    |[0, 1000),[-100, 100)
id   |empty |empty    |[0, 1000),[1000, 1200)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_4)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem:[-100, 100)");
    {
        ASSERT_EQ(1100, row_id->size());
        auto except = genSequence<UInt32>("[1000, 1200)|[100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1100));
    }
    {
        ASSERT_EQ(1100, handle->size());
        auto except = genSequence<Int64>("[-100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1100));
    }
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_5)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
    ASSERT_EQ(nullptr, row_id);
    ASSERT_EQ(nullptr, handle);
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_6)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem_del:[100, 200)");
    {
        ASSERT_EQ(900, row_id->size());
        auto except = genSequence<UInt32>("[0, 100)|[200, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 900));
    }
    {
        ASSERT_EQ(900, handle->size());
        auto except = genSequence<Int64>("[0, 100)|[200, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 900));
    }
}
CATCH

TEST_F(SegmentBitmapFilterTest, InMemory_7)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
    {
        ASSERT_EQ(900, row_id->size());
        auto except = genSequence<UInt32>("[100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 900));
    }
    {
        ASSERT_EQ(900, handle->size());
        auto except = genSequence<Int64>("[100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 900));
    }
}
CATCH

TEST_F(SegmentBitmapFilterTest, Tiny_1)
try
{
    auto [row_id, handle] = writeSegment("d_tiny:[100, 500)|d_mem:[200, 1000)");
    {
        ASSERT_EQ(900, row_id->size());
        auto except = genSequence<UInt32>("[0, 100)|[400, 1200)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 900));
    }
    {
        ASSERT_EQ(900, handle->size());
        auto except = genSequence<Int64>("[100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 900));
    }
}
CATCH

TEST_F(SegmentBitmapFilterTest, TinyDel_1)
try
{
    auto [row_id, handle] = writeSegment("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
    {
        ASSERT_EQ(400, row_id->size());
        auto except = genSequence<UInt32>("[500, 600)|[0, 100)|[200, 400)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 400));
    }
    {
        ASSERT_EQ(400, handle->size());
        auto except = genSequence<Int64>("[0, 200)|[300, 500)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 400));
    }
}
CATCH

/*

TEST_F(SegmentBitmapFilterTest, InMemory_3)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem:[100, 200)");
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[0, 100)|[1000, 1100)|[200, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1000));
    }
}
CATCH


TEST_F(SegmentBitmapFilterTest, InMemory_4)
try
{
    auto [row_id, handle] = writeSegment("d_mem:[0, 1000)|d_mem:[-100, 100)");
    {
        ASSERT_EQ(1100, row_id->size());
        auto except = genSequence<UInt32>("[1000, 1200)|[100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), row_id->data(), 1100));
    }
    {
        ASSERT_EQ(1100, handle->size());
        auto except = genSequence<Int64>("[-100, 1000)");
        ASSERT_TRUE(sequenceEqual(except.data(), handle->data(), 1100));
    }
}
CATCH
*/
} // namespace DB::DM::tests

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

#include <future>

using namespace std::chrono_literals;
using namespace DB::tests;

namespace DB::DM::tests
{

template<typename E, typename A>
::testing::AssertionResult sequenceEqual(const E * except, const A * actual, size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        if (except[i] != actual[i])
        {
            return ::testing::AssertionFailure()
                << fmt::format("Value at index {} mismatch: except {} vs actual {}. except => {} actual => {}",
                               i, except[i], actual[i],
                               std::vector<E>(except, except + size),
                               std::vector<A>(actual, actual + size));
        }
    }
    return ::testing::AssertionSuccess();
}

template<typename T>
std::vector<T> genSequence(T start, T end)
{
    auto size = end - start;
    std::vector<T> v(size);
    std::iota(v.begin(), v.end(), start);
    return v;
}

template<typename T>
std::vector<T> genSequence(const std::vector<std::pair<T, T>> & ranges)
{
    std::vector<T> res;
    for (auto [start, end] : ranges)
    {
        auto v = genSequence(start, end);
        res.insert(res.end(), v.begin(), v.end());
    }
    return res;
}

// "[a, b)|[c, d)
template<typename T>
std::vector<std::pair<T, T>> readableRangesToVectorRanges(std::string_view readable_ranges)
{
    std::vector<std::string> ranges;
    boost::split(ranges, readable_ranges, boost::is_any_of("|"));
    RUNTIME_CHECK(!ranges.empty());
    std::vector<std::pair<T, T>> vector_ranges;
    for (auto & r : ranges)
    {
        boost::algorithm::trim(r);
        RUNTIME_CHECK(r.front() == '[' && r.back() == ')', r);
        std::vector<std::string> boundaries;
        r = r.substr(1, r.size() - 2);
        boost::split(boundaries, r, boost::is_any_of(","));
        RUNTIME_CHECK(boundaries.size() == 2, r);
        auto start = static_cast<T>(std::stol(boundaries[0]));
        auto end = static_cast<T>(std::stol(boundaries[1]));
        vector_ranges.push_back({start, end});
    }
    return vector_ranges;
}

template<typename T>
std::vector<T> genSequence(std::string_view readable_ranges)
{
    auto vector_ranges = readableRangesToVectorRanges<T>(readable_ranges);
    return genSequence(vector_ranges);
}

constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;

class SegmentBitmapFilterTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentBitmapFilterTest");
};

/*
0----------------stable_rows----------------stable_rows + delta_rows
| stable value space | delta value space .......................... <-- append
|--------------------|--ColumnFilePersisted--|ColumnFileInMemory... <-- append
|--------------------|-Tiny|DeleteRange|Big--|ColumnFileInMemory... <-- append
*/


/*
     |stable|Persisted|InMemory|
data |empty |empty    |[0, 1000)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_1)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, /* at */ 0);
    auto row_id = getSegmentRowId(SEG_ID);
    auto handle = getSegmentHandle(SEG_ID);
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[0, 1000)");
        const auto * actual = toColumnVectorDataPtr<UInt32>(row_id);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        const auto * actual = toColumnVectorDataPtr<Int64>(handle);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
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
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, /* at */ 0);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, /* at */ 0);
    auto row_id = getSegmentRowId(SEG_ID);
    auto handle = getSegmentHandle(SEG_ID);
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[1000, 2000)");
        const auto * actual = toColumnVectorDataPtr<UInt32>(row_id);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        const auto * actual = toColumnVectorDataPtr<Int64>(handle);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
    }
}
CATCH

/*
     |stable|Persisted|InMemory
data |empty |empty    |[0, 1000),[100, 200)
id   |empty |empty    |[0, 1000),[1000, 1100)
*/
TEST_F(SegmentBitmapFilterTest, InMemory_2_1)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, /* at */ 0);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 100);
    auto row_id = getSegmentRowId(SEG_ID);
    auto handle = getSegmentHandle(SEG_ID);
    {
        ASSERT_EQ(1000, row_id->size());
        auto except = genSequence<UInt32>("[0, 100)|[1000, 1100)|[200, 1000)");
        const auto * actual = toColumnVectorDataPtr<UInt32>(row_id);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
    }
    {
        ASSERT_EQ(1000, handle->size());
        auto except = genSequence<Int64>("[0, 1000)");
        const auto * actual = toColumnVectorDataPtr<Int64>(handle);
        ASSERT_TRUE(sequenceEqual(except.data(), actual->data(), 1000));
    }
}
CATCH
}

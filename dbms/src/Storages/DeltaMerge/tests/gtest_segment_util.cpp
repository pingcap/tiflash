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

#include <Columns/ColumnsCommon.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <boost_wrapper/string.h>

namespace DB::DM::tests
{

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
    vector_ranges.reserve(ranges.size());
    for (auto & r : ranges)
    {
        vector_ranges.emplace_back(parseRange<T>(r));
    }
    return vector_ranges;
}

// "type:[a, b)" => SegDataUnit
SegDataUnit parseSegDataUnit(String & s)
{
    boost::algorithm::trim(s);
    std::vector<String> values;
    boost::split(values, s, boost::is_any_of(":"));
    if (values.size() == 2)
    {
        return SegDataUnit{
            .type = boost::algorithm::trim_copy(values[0]),
            .range = parseRange<Int64>(values[1]),
        };
    }
    else if (values.size() == 3)
    {
        RUNTIME_CHECK(values[0] == "d_big" || values[0] == "s", s);
        return SegDataUnit{
            .type = boost::algorithm::trim_copy(values[0]),
            .range = parseRange<Int64>(values[1]),
            .pack_size = std::stoul(values[2]),
        };
    }
    RUNTIME_CHECK_MSG(false, "parseSegDataUnit failed: {}", s);
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
    seg_data_units.reserve(str_seg_data_units.size());
    for (auto & s : str_seg_data_units)
    {
        seg_data_units.emplace_back(parseSegDataUnit(s));
    }
    check(seg_data_units);
    return seg_data_units;
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

template <typename T>
std::vector<T> genSequence(std::string_view str_ranges)
{
    auto vector_ranges = parseRanges<T>(str_ranges);
    return genSequence(vector_ranges);
}

template <typename E, typename A>
::testing::AssertionResult sequenceEqual(const E * expected, const A * actual, size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        if (expected[i] != actual[i])
        {
            return ::testing::AssertionFailure() << fmt::format(
                       "Value at index {} mismatch: expected {} vs actual {}. expected => {} actual => {}",
                       i,
                       expected[i],
                       actual[i],
                       std::vector<E>(expected, expected + size),
                       std::vector<A>(actual, actual + size));
        }
    }
    return ::testing::AssertionSuccess();
}

template std::vector<Int64> genSequence(std::string_view str_ranges);
template std::vector<UInt32> genSequence(std::string_view str_ranges);
template ::testing::AssertionResult sequenceEqual(const UInt32 * expected, const UInt32 * actual, size_t size);
template ::testing::AssertionResult sequenceEqual(const Int64 * expected, const Int64 * actual, size_t size);

} // namespace DB::DM::tests

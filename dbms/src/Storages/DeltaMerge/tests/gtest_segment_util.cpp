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

#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <boost_wrapper/string.h>

namespace DB::DM::tests
{

namespace
{
template <typename T>
std::tuple<T, T, bool> parseRange(String & str_range)
{
    boost::algorithm::trim(str_range);
    RUNTIME_CHECK(str_range.front() == '[' && (str_range.back() == ')' || str_range.back() == ']'), str_range);
    std::vector<String> values;
    const auto left_right = str_range.substr(1, str_range.size() - 2);
    boost::split(values, left_right, boost::is_any_of(","));
    RUNTIME_CHECK(values.size() == 2, left_right);
    return {static_cast<T>(std::stol(values[0])), static_cast<T>(std::stol(values[1])), str_range.back() == ']'};
}

// "[a, b)|[c, d)" => [std::pair{a, b}, std::pair{c, d}]
template <typename T>
std::vector<std::tuple<T, T, bool>> parseRanges(std::string_view str_ranges)
{
    std::vector<String> ranges;
    boost::split(ranges, str_ranges, boost::is_any_of("|"));
    RUNTIME_CHECK(!ranges.empty(), str_ranges);
    std::vector<std::tuple<T, T, bool>> vector_ranges;
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
    for (auto & v : values)
        boost::algorithm::trim(v);
    RUNTIME_CHECK(values.size() >= 2, s, values);
    SegDataUnit unit;
    unit.type = values[0];
    unit.range = parseRange<Int64>(values[1]);
    for (size_t i = 2; i < values.size(); i++)
    {
        // Pack size for DMFile
        std::string_view attr_pack_size_prefix{"pack_size_"};
        if (values[i].starts_with(attr_pack_size_prefix))
        {
            RUNTIME_CHECK(unit.type == "d_big" || unit.type == "s", s, unit.type);
            unit.pack_size = std::stoul(values[i].substr(attr_pack_size_prefix.size()));
            continue;
        }
        
        // Make data in ColumnFileTiny or ColumnFileMem unsorted.
        std::string_view attr_shuffle{"shuffle"};
        if (values[i] == attr_shuffle)
        {
            RUNTIME_CHECK(unit.type == "d_mem" || unit.type == "d_tiny", s, unit.type);
            unit.shuffle = true;
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: {} is unsupported", s, values[i]);
    }

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
        auto [begin, end, including_right_boundary] = seg_data_units[i].range;
        RUNTIME_CHECK(end - begin + including_right_boundary > 0, begin, end, including_right_boundary);
    }
    RUNTIME_CHECK(stable_units.empty() || (stable_units.size() == 1 && stable_units[0] == 0));
    std::vector<size_t> expected_mem_units(mem_units.size());
    std::iota(expected_mem_units.begin(), expected_mem_units.end(), seg_data_units.size() - mem_units.size());
    RUNTIME_CHECK(mem_units == expected_mem_units, expected_mem_units, mem_units);
}

template <typename T>
std::vector<T> genSequence(T begin, T end, bool including_right_boundary)
{
    auto size = end - begin + including_right_boundary;
    std::vector<T> v(size);
    std::iota(v.begin(), v.end(), begin);
    return v;
}
} // namespace

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
std::vector<T> genSequence(const std::vector<std::tuple<T, T, bool>> & ranges)
{
    std::vector<T> res;
    for (auto [begin, end, including_right_boundary] : ranges)
    {
        auto v = genSequence(begin, end, including_right_boundary);
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

template std::vector<Int64> genSequence(std::string_view str_ranges);
template std::vector<UInt32> genSequence(std::string_view str_ranges);

} // namespace DB::DM::tests

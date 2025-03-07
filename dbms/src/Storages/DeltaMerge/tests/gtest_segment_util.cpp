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
Strings splitAndTrim(std::string_view s, std::string_view delimiter, std::optional<size_t> expected_size = std::nullopt)
{
    Strings results;
    boost::split(results, s, boost::is_any_of(delimiter));
    if (expected_size)
        RUNTIME_CHECK(results.size() == *expected_size, s, delimiter, expected_size);
    else
        RUNTIME_CHECK(!results.empty(), s, delimiter);
    for (auto & r : results)
        boost::trim(r);
    return results;
}

// [a, b) => {a, b, false}
// [a, b] => {a, b, true}
template <typename T>
std::tuple<T, T, bool> parseRange(std::string_view s)
{
    auto str_range = boost::trim_copy(s);
    RUNTIME_CHECK(str_range.front() == '[' && (str_range.back() == ')' || str_range.back() == ']'), str_range);
    auto values = splitAndTrim(str_range.substr(1, str_range.size() - 2), ",", 2);
    return {static_cast<T>(std::stol(values[0])), static_cast<T>(std::stol(values[1])), str_range.back() == ']'};
}

// "[a, b)|[c, d]" => [{a, b, false}, {c, d, true}]
template <typename T>
std::vector<std::tuple<T, T, bool>> parseRanges(std::string_view s)
{
    auto str_range = boost::trim_copy(s);
    RUNTIME_CHECK(str_range.front() == '[' && (str_range.back() == ')' || str_range.back() == ']'), str_range);
    auto ranges = splitAndTrim(str_range, "|");
    std::vector<std::tuple<T, T, bool>> vector_ranges;
    vector_ranges.reserve(ranges.size());
    for (auto & r : ranges)
        vector_ranges.emplace_back(parseRange<T>(r));
    return vector_ranges;
}

const std::unordered_set<std::string_view> segment_commands = {"flush_cache", "compact_delta", "merge_delta"};
const std::unordered_set<std::string_view> delta_small_data_types = {"d_mem", "d_mem_del", "d_tiny", "d_tiny_del"};
const std::unordered_set<std::string_view> segment_data_types
    = {"d_mem", "d_mem_del", "d_tiny", "d_tiny_del", "d_dr", "s"};

void parseSegUnitAttr(std::string_view attr, SegDataUnit & unit)
{
    // Pack size for DMFile
    static const std::string_view attr_pack_size_prefix{"pack_size_"};
    // Shuffle data ColumnFileTiny and ColumnFileMemory
    static const std::string_view attr_shuffle{"shuffle"};
    // Timestamp for generated data
    static const std::string_view attr_timestamp_prefix{"ts_"};

    if (attr.starts_with(attr_pack_size_prefix))
    {
        RUNTIME_CHECK(unit.type == "d_big" || unit.type == "s" || unit.type == "merge_delta", attr, unit.type);
        unit.pack_size = std::stoul(String(attr.substr(attr_pack_size_prefix.size())));
        return;
    }

    if (attr == attr_shuffle)
    {
        RUNTIME_CHECK(delta_small_data_types.contains(unit.type), attr, unit.type, delta_small_data_types);
        unit.shuffle = true;
        return;
    }

    if (attr.starts_with(attr_timestamp_prefix))
    {
        RUNTIME_CHECK(
            delta_small_data_types.contains(unit.type) || unit.type == "s",
            attr,
            unit.type,
            delta_small_data_types);
        unit.ts = std::stoul(String(attr.substr(attr_timestamp_prefix.size())));
        return;
    }

    RUNTIME_CHECK_MSG(false, "{} is unsupported", attr);
}

// data_type:[left, right):attr1:attr2
// cmd_type:attr1:attr2
SegDataUnit parseSegDataUnit(std::string_view s)
{
    auto s_trim = boost::trim_copy(s);
    auto values = splitAndTrim(s_trim, ":");
    size_t i = 0;
    SegDataUnit unit{.type = values[i++]};
    if (!segment_commands.contains(unit.type))
    {
        RUNTIME_CHECK(values.size() >= i, s, values);
        unit.range = parseRange<Int64>(values[i++]);
    }
    for (; i < values.size(); i++)
        parseSegUnitAttr(values[i], unit);
    return unit;
}

void check(const std::vector<SegDataUnit> & seg_data_units)
{
    RUNTIME_CHECK(!seg_data_units.empty());
    std::vector<size_t> stable_units;
    std::vector<size_t> mem_units;
    for (size_t i = 0; i < seg_data_units.size(); i++)
    {
        const auto & type = seg_data_units[i].type;
        if (segment_commands.contains(type))
            continue;

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
    // If stable exists, it should be the first one.
    RUNTIME_CHECK(stable_units.empty() || (stable_units.size() == 1 && stable_units[0] == 0));
}

template <typename T>
std::vector<T> genSequence(T begin, T end, bool including_right_boundary)
{
    auto size = end - begin + including_right_boundary;
    std::vector<T> v(size);
    std::iota(v.begin(), v.end(), begin);
    return v;
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
} // namespace

std::vector<SegDataUnit> parseSegData(std::string_view seg_data)
{
    auto str_seg_data_units = splitAndTrim(seg_data, "|");
    std::vector<SegDataUnit> seg_data_units;
    seg_data_units.reserve(str_seg_data_units.size());
    for (const auto & s : str_seg_data_units)
    {
        seg_data_units.emplace_back(parseSegDataUnit(s));
    }
    check(seg_data_units);
    return seg_data_units;
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

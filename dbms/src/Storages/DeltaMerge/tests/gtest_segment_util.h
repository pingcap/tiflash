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

#pragma once

#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <common/defines.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

// "[a, b)" => std::tuple{a, b, false}
// "[a, b]" => std::tuple{a, b, true}
template <typename T>
std::tuple<T, T, bool> parseRange(String & str_range);

// "[a, b)|[c, d]" => [std::tuple{a, b, false}, std::tuple{c, d, true}]
template <typename T>
std::vector<std::tuple<T, T, bool>> parseRanges(std::string_view str_ranges);

struct SegDataUnit
{
    String type;
    std::tuple<Int64, Int64, bool> range; // {left, right, including_right_boundary}
    std::optional<size_t> pack_size; // For DMFile
};

// "type:[a, b)" => SegDataUnit
SegDataUnit parseSegDataUnit(String & s);

void check(const std::vector<SegDataUnit> & seg_data_units);

std::vector<SegDataUnit> parseSegData(std::string_view seg_data);

template <typename T>
std::vector<T> genSequence(T begin, T end, bool including_right_boundary);

template <typename T>
std::vector<T> genSequence(const std::vector<std::pair<T, T>> & ranges);

template <typename T>
std::vector<T> genSequence(std::string_view str_ranges);

template <typename T>
std::vector<T> genHandleSequence(std::string_view str_ranges)
{
    auto v = genSequence<Int64>(str_ranges);
    if constexpr (std::is_same_v<T, Int64>)
        return v;
    else
    {
        static_assert(std::is_same_v<T, String>);
        std::vector<String> res(v.size());
        for (size_t i = 0; i < v.size(); i++)
            res[i] = genMockCommonHandle(v[i], 1);
        return res;
    }
}

template <typename E, typename A>
::testing::AssertionResult sequenceEqual(const E & expected, const A & actual)
{
    if (expected.size() != actual.size())
    {
        return ::testing::AssertionFailure()
            << fmt::format("Size mismatch: expected {} vs actual {}.", expected.size(), actual.size());
    }
    for (size_t i = 0; i < expected.size(); i++)
    {
        if (expected[i] != actual[i])
        {
            return ::testing::AssertionFailure() << fmt::format(
                       "Value at index {} mismatch: expected {} vs actual {}. expected => {} actual => {}",
                       i,
                       expected[i],
                       actual[i],
                       expected,
                       actual);
        }
    }
    return ::testing::AssertionSuccess();
}

} // namespace DB::DM::tests

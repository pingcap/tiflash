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

// If including_right_boundary is false, it means [left, right).
// If including_right_boundary is true, it means [left, right].
// `including_right_boundary` is required if we want to generate data with std::numeric_limits<T>::max().
// Theoretically, we could enforce the use of closed intervals, thereby eliminating the need for the parameter 'including_right_boundary'.
// However, a multitude of existing tests are predicated on the assumption that the interval is left-closed and right-open.
template <typename T>
struct SegDataRange
{
    T left;
    T right;
    bool including_right_boundary;
};

struct SegDataUnit
{
    String type;
    SegDataRange<Int64> range;
    std::optional<size_t> pack_size; // For DMFile
    bool shuffle = false; // For ColumnFileTiny and ColumnFileMemory
    std::optional<UInt64> ts;
};

std::vector<SegDataUnit> parseSegData(std::string_view seg_data);

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

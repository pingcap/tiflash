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

#include <common/defines.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{
struct SegDataUnit
{
    String type;
    std::pair<Int64, Int64> range; // Data range
    std::optional<size_t> pack_size; // For DMFile
};

std::vector<SegDataUnit> parseSegData(std::string_view seg_data);

template <typename T>
std::vector<T> genSequence(std::string_view str_ranges);

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

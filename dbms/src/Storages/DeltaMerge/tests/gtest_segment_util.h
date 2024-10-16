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

// "[a, b)" => std::pair{a, b}
template <typename T>
std::pair<T, T> parseRange(String & str_range);

// "[a, b)|[c, d)" => [std::pair{a, b}, std::pair{c, d}]
template <typename T>
std::vector<std::pair<T, T>> parseRanges(std::string_view str_ranges);

struct SegDataUnit
{
    String type;
    std::pair<Int64, Int64> range; // Data range
    std::optional<size_t> pack_size; // For DMFile
};

// "type:[a, b)" => SegDataUnit
SegDataUnit parseSegDataUnit(String & s);

void check(const std::vector<SegDataUnit> & seg_data_units);

std::vector<SegDataUnit> parseSegData(std::string_view seg_data);

template <typename T>
std::vector<T> genSequence(T begin, T end);

template <typename T>
std::vector<T> genSequence(const std::vector<std::pair<T, T>> & ranges);

template <typename T>
std::vector<T> genSequence(std::string_view str_ranges);

template <typename E, typename A>
::testing::AssertionResult sequenceEqual(const E * expected, const A * actual, size_t size);

} // namespace DB::DM::tests

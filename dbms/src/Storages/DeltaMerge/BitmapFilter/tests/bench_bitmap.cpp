// Copyright 2024 PingCAP, Inc.
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

#include <Core/Defines.h>
#include <benchmark/benchmark.h>

#include <random>
#include <vector>

namespace DB::bench
{
constexpr size_t TEST_BITMAP_SIZE = 65536;

template <typename T>
void bitmapAndStd(benchmark::State & state)
{
    std::vector<T> a(TEST_BITMAP_SIZE);
    std::vector<T> b(TEST_BITMAP_SIZE);
    for (auto _ : state)
    {
        std::array<T, TEST_BITMAP_SIZE> c;
        std::transform(a.begin(), a.end(), b.begin(), c.begin(), [](const auto i, const auto j) { return i && j; });
        benchmark::DoNotOptimize(c);
    }
}

static void bitmapAndBool(benchmark::State & state)
{
    bitmapAndStd<bool>(state);
}

static void bitmapAndUInt8(benchmark::State & state)
{
    bitmapAndStd<UInt8>(state);
}

std::vector<UInt32> genRandomRowIDs(size_t rowid_count, size_t bitmap_size)
{
    std::vector<UInt32> row_ids(rowid_count);
    std::random_device rd;
    std::mt19937 gen(rd());
    for (auto & id : row_ids)
    {
        id = gen() % bitmap_size;
    }
    return row_ids;
}

template <typename T>
void bitmapSetRowID(benchmark::State & state)
{
    constexpr size_t rowid_count = 45678;
    auto row_ids = genRandomRowIDs(rowid_count, TEST_BITMAP_SIZE);
    for (auto _ : state)
    {
        std::vector<T> v(TEST_BITMAP_SIZE, static_cast<T>(0));
        for (auto id : row_ids)
        {
            v[id] = static_cast<T>(1);
        }
        benchmark::DoNotOptimize(v);
    }
}

static void bitmapSetRowIDBool(benchmark::State & state)
{
    bitmapSetRowID<bool>(state);
}

static void bitmapSetRowIDUInt8(benchmark::State & state)
{
    bitmapSetRowID<UInt8>(state);
}

template <typename T>
std::vector<T> buildBitmapByRanges(const std::vector<std::pair<size_t, size_t>> & ranges, size_t bitmap_size)
{
    std::vector<T> v(bitmap_size, static_cast<T>(0));
    for (auto [start, limit] : ranges)
    {
        std::fill_n(v.begin() + start, limit, 1);
    }
    return v;
}

template <typename T>
void bitmapSetRange(benchmark::State & state)
{
    const std::vector<std::pair<size_t, size_t>> set_ranges = {
        {0, 8192},
        {8192 * 2, 8192},
        {8192 * 5, 8192},
    };
    for (auto _ : state)
    {
        auto v = buildBitmapByRanges<T>(set_ranges, TEST_BITMAP_SIZE);
        benchmark::DoNotOptimize(v);
    }
}

static void bitmapSetRangeBool(benchmark::State & state)
{
    bitmapSetRange<bool>(state);
}

static void bitmapSetRangeUInt8(benchmark::State & state)
{
    bitmapSetRange<UInt8>(state);
}

template <typename T>
void bitmapGetRange(benchmark::State & state)
{
    const std::vector<std::pair<size_t, size_t>> set_ranges = {
        {0, 8192},
        {8192 * 2, 8192},
        {8192 * 5, 8192},
    };
    auto v = buildBitmapByRanges<T>(set_ranges, TEST_BITMAP_SIZE);

    const std::vector<std::pair<size_t, size_t>> get_ranges = {
        {1234, 8192},
        {8192 * 2 + 1234, 8192},
        {8192 * 5 + 1234, 8192},
    };
    for (auto _ : state)
    {
        std::array<T, 8192> f;
        for (auto [start, limit] : get_ranges)
        {
            std::copy(v.begin() + start, v.begin() + start + limit, f.begin());
        }
        benchmark::DoNotOptimize(f);
    }
}

static void bitmapGetRangeBool(benchmark::State & state)
{
    bitmapGetRange<bool>(state);
}

static void bitmapGetRangeUInt8(benchmark::State & state)
{
    bitmapGetRange<UInt8>(state);
}

BENCHMARK(bitmapAndBool);
BENCHMARK(bitmapAndUInt8);
BENCHMARK(bitmapSetRowIDBool);
BENCHMARK(bitmapSetRowIDUInt8);
BENCHMARK(bitmapSetRangeBool);
BENCHMARK(bitmapSetRangeUInt8);
BENCHMARK(bitmapGetRangeBool);
BENCHMARK(bitmapGetRangeUInt8);
} // namespace DB::bench

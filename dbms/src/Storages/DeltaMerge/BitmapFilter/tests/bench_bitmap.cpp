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
template <typename T>
void bitmapAndStd(benchmark::State & state)
{
    std::vector<T> a(65536);
    std::vector<T> b(65536);
    for (auto _ : state)
    {
        std::vector<T> c(65536);
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

template <typename T>
void bitmapSetRowID(benchmark::State & state)
{
    constexpr size_t bitmap_size = 65536;
    constexpr size_t rowid_size = 45678;

    std::vector<UInt32> row_ids(rowid_size);
    std::random_device rd;
    std::mt19937 gen(rd());
    for (auto & id : row_ids)
    {
        id = gen() % bitmap_size;
    }
    for (auto _ : state)
    {
        std::vector<T> v(bitmap_size, static_cast<T>(0));
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

BENCHMARK(bitmapAndBool);
BENCHMARK(bitmapAndUInt8);
BENCHMARK(bitmapSetRowIDBool);
BENCHMARK(bitmapSetRowIDUInt8);
} // namespace DB::bench

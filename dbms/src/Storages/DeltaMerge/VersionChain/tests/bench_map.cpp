// Copyright 2025 PingCAP, Inc.
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

#include <benchmark/benchmark.h>
#include <common/types.h>

#include <map>
#include <random>
#include <unordered_map>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#pragma clang diagnostic pop

#include <fmt/format.h>

namespace
{
constexpr int map_size = 100000;

template <typename MapType>
void BM_MapInsert(benchmark::State & state)
{
    std::random_device rd;
    std::mt19937 g(rd());
    for (auto _ : state)
    {
        MapType map;
        for (UInt32 i = 0; i < state.range(0); ++i)
            map.insert({g(), i});
        benchmark::DoNotOptimize(map);
    }
}

template <typename MapType>
void BM_MapFind(benchmark::State & state)
{
    std::random_device rd;
    std::mt19937 g(rd());
    MapType map;
    for (UInt32 i = 0; i < state.range(0); ++i)
        map.insert({g(), i});
    for (auto _ : state)
    {
        for (UInt32 i = 0; i < state.range(0); ++i)
            benchmark::DoNotOptimize(map.find(g()));
    }
}

BENCHMARK_TEMPLATE(BM_MapInsert, std::map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapInsert, std::unordered_map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapInsert, absl::btree_map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapInsert, absl::flat_hash_map<Int64, UInt32>)->Arg(map_size);

BENCHMARK_TEMPLATE(BM_MapFind, std::map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapFind, std::unordered_map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapFind, absl::btree_map<Int64, UInt32>)->Arg(map_size);
BENCHMARK_TEMPLATE(BM_MapFind, absl::flat_hash_map<Int64, UInt32>)->Arg(map_size);

} // namespace

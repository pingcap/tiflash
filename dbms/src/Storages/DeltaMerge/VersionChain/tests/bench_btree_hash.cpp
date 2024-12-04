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


#include <benchmark/benchmark.h>
#include <common/types.h>

#include <functional>
#include <random>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#pragma clang diagnostic pop

namespace
{

enum class IndexType
{
    BTree = 0,
    Hash = 1,
};

enum class SearchResult
{
    AlwaysFound = 0,
    AlwaysNotFound = 1,
};

std::vector<std::pair<Int64, UInt32>> randomData(UInt32 size)
{
    std::vector<std::pair<Int64, UInt32>> v(size);
    for (UInt32 i = 0; i < size; ++i)
    {
        v[i] = {i, i};
    }
    std::shuffle(v.begin(), v.end(), std::mt19937(std::random_device()()));
    return v;
}

template <typename... Args>
void build(benchmark::State & state, Args &&... args)
{
    const auto [type, size] = std::make_tuple(std::move(args)...);
    const auto v = randomData(size);

    if (type == IndexType::BTree)
    {
        for (auto _ : state)
        {
            absl::btree_map<UInt64, UInt32> t(v.begin(), v.end());
            benchmark::DoNotOptimize(t);
        }
    }
    else
    {
        for (auto _ : state)
        {
            absl::flat_hash_map<UInt64, UInt32> t(v.begin(), v.end());
            benchmark::DoNotOptimize(t);
        }
    }
}

template <typename... Args>
void search(benchmark::State & state, Args &&... args)
{
    const auto [index_type, search_result, size] = std::make_tuple(std::move(args)...);
    const auto v = randomData(size);

    std::random_device rd;
    std::mt19937 g(rd());

    auto gen_always_found = [&]() {
        return g() % size;
    };
    auto gen_always_not_found = [&]() {
        return gen_always_found() + size;
    };
    std::function<long()> gen;
    if (search_result == SearchResult::AlwaysFound)
        gen = gen_always_found;
    else
        gen = gen_always_not_found;

    if (index_type == IndexType::BTree)
    {
        absl::btree_map<UInt64, UInt32> t(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = t.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else
    {
        absl::flat_hash_map<UInt64, UInt32> t(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = t.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
}

BENCHMARK_CAPTURE(search, btree_found_10000, IndexType::BTree, SearchResult::AlwaysFound, 10000);
BENCHMARK_CAPTURE(search, btree_found_20000, IndexType::BTree, SearchResult::AlwaysFound, 20000);
BENCHMARK_CAPTURE(search, btree_found_40000, IndexType::BTree, SearchResult::AlwaysFound, 40000);
BENCHMARK_CAPTURE(search, btree_found_80000, IndexType::BTree, SearchResult::AlwaysFound, 80000);
BENCHMARK_CAPTURE(search, hash_found_10000, IndexType::Hash, SearchResult::AlwaysFound, 10000);
BENCHMARK_CAPTURE(search, hash_found_20000, IndexType::Hash, SearchResult::AlwaysFound, 20000);
BENCHMARK_CAPTURE(search, hash_found_40000, IndexType::Hash, SearchResult::AlwaysFound, 40000);
BENCHMARK_CAPTURE(search, hash_found_80000, IndexType::Hash, SearchResult::AlwaysFound, 80000);

BENCHMARK_CAPTURE(search, btree_not_found_10000, IndexType::BTree, SearchResult::AlwaysNotFound, 10000);
BENCHMARK_CAPTURE(search, btree_not_found_20000, IndexType::BTree, SearchResult::AlwaysNotFound, 20000);
BENCHMARK_CAPTURE(search, btree_not_found_40000, IndexType::BTree, SearchResult::AlwaysNotFound, 40000);
BENCHMARK_CAPTURE(search, btree_not_found_80000, IndexType::BTree, SearchResult::AlwaysNotFound, 80000);
BENCHMARK_CAPTURE(search, hash_not_found_10000, IndexType::Hash, SearchResult::AlwaysNotFound, 10000);
BENCHMARK_CAPTURE(search, hash_not_found_20000, IndexType::Hash, SearchResult::AlwaysNotFound, 20000);
BENCHMARK_CAPTURE(search, hash_not_found_40000, IndexType::Hash, SearchResult::AlwaysNotFound, 40000);
BENCHMARK_CAPTURE(search, hash_not_found_80000, IndexType::Hash, SearchResult::AlwaysNotFound, 80000);

BENCHMARK_CAPTURE(build, btree, IndexType::BTree, 10000);
BENCHMARK_CAPTURE(build, btree, IndexType::BTree, 20000);
BENCHMARK_CAPTURE(build, btree, IndexType::BTree, 40000);
BENCHMARK_CAPTURE(build, btree, IndexType::BTree, 80000);
BENCHMARK_CAPTURE(build, hash, IndexType::Hash, 10000);
BENCHMARK_CAPTURE(build, hash, IndexType::Hash, 20000);
BENCHMARK_CAPTURE(build, hash, IndexType::Hash, 40000);
BENCHMARK_CAPTURE(build, hash, IndexType::Hash, 80000);

} // namespace

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
//#include <absl/container/btree_set.h>
#include <functional>
#include <random>
namespace
{

enum class SearchType
{
    LinearScan = 0,
    BinarySearch = 1,
};

enum class SearchResult
{
    AlwaysFound = 0,
    AlwaysNotFound = 1,
};

template <typename... Args>
void searchInt64(benchmark::State & state, Args &&... args)
{
    const auto [type, size, result] = std::make_tuple(std::move(args)...);
    std::vector<std::int64_t> v(size);
    std::iota(v.begin(), v.end(), 0);
    std::random_device rd;
    std::mt19937 g(rd());

    auto gen_always_found = [&]() {
        return g() % size;
    };
    auto gen_always_not_found = [&]() {
        return gen_always_found() + size;
    };
    std::function<long()> gen;
    if (result == SearchResult::AlwaysFound)
        gen = gen_always_found;
    else
        gen = gen_always_not_found;

    if (type == SearchType::BinarySearch)
    {
        for (auto _ : state)
        {
            auto itr = std::lower_bound(v.begin(), v.end(), gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (type == SearchType::LinearScan)
    {
        for (auto _ : state)
        {
            auto itr = std::find(v.begin(), v.end(), gen());
            benchmark::DoNotOptimize(itr);
        }
    }

    /*
    else if (search_type == SearchType::StdSet)
    {
        std::set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::AbslSet)
    {
        absl::btree_set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }*/
}

struct Entry
{
    std::int64_t id, a, b;
};

template <typename... Args>
void searchStruct(benchmark::State & state, Args &&... args)
{
    const auto [type, size, result] = std::make_tuple(std::move(args)...);
    std::vector<Entry> v(size);
    for (int i = 0; i < size; ++i)
        v[i].id = i;
    std::random_device rd;
    std::mt19937 g(rd());

    auto gen_always_found = [&]() {
        return g() % size;
    };
    auto gen_always_not_found = [&]() {
        return gen_always_found() + size;
    };
    std::function<long()> gen;
    if (result == SearchResult::AlwaysFound)
        gen = gen_always_found;
    else
        gen = gen_always_not_found;

    if (type == SearchType::BinarySearch)
    {
        for (auto _ : state)
        {
            auto itr
                = std::lower_bound(v.begin(), v.end(), gen(), [](const Entry & e, std::int64_t v) { return e.id < v; });
            benchmark::DoNotOptimize(itr);
        }
    }
    /*
    else if (type == SearchType::LinearScan)
    {
        for (auto _ : state)
        {
            auto itr = std::find(v.begin(), v.end(), gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::StdSet)
    {
        std::set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }
    else if (search_type == SearchType::AbslSet)
    {
        absl::btree_set<long> s(v.begin(), v.end());
        for (auto _ : state)
        {
            auto itr = s.find(gen());
            benchmark::DoNotOptimize(itr);
        }
    }*/
}
/*
BENCHMARK_CAPTURE(searchInt64, LinearScan4, SearchType::LinearScan, 4, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan8, SearchType::LinearScan, 8, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan16, SearchType::LinearScan, 16, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan32, SearchType::LinearScan, 32, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan64, SearchType::LinearScan, 64, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan128, SearchType::LinearScan, 128, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan256, SearchType::LinearScan, 256, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, LinearScan512, SearchType::LinearScan, 512, SearchResult::AlwaysFound);
*/

BENCHMARK_CAPTURE(searchInt64, BinarySearch4, SearchType::BinarySearch, 4, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch8, SearchType::BinarySearch, 8, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch16, SearchType::BinarySearch, 16, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch32, SearchType::BinarySearch, 32, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch64, SearchType::BinarySearch, 64, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch128, SearchType::BinarySearch, 128, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch256, SearchType::BinarySearch, 256, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchInt64, BinarySearch512, SearchType::BinarySearch, 512, SearchResult::AlwaysFound);


BENCHMARK_CAPTURE(searchStruct, BinarySearch4, SearchType::BinarySearch, 4, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch8, SearchType::BinarySearch, 8, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch16, SearchType::BinarySearch, 16, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch32, SearchType::BinarySearch, 32, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch64, SearchType::BinarySearch, 64, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch128, SearchType::BinarySearch, 128, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch256, SearchType::BinarySearch, 256, SearchResult::AlwaysFound);
BENCHMARK_CAPTURE(searchStruct, BinarySearch512, SearchType::BinarySearch, 512, SearchResult::AlwaysFound);

} // namespace

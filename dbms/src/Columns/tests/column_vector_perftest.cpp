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

#include <Columns/ColumnVector.h>
#include <common/types.h>
#include <fmt/core.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <thread>

namespace DB::tests
{
namespace
{
std::random_device rd;

using StopFlag = std::atomic<bool>;

template <typename T>
ColumnPtr buildColumn(int n)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<T> dist;

    auto res = ColumnVector<T>::create();
    auto & container = res->getData();

    for (int i = 0; i < n; ++i)
        container.push_back(dist(mt));

    return res;
}

IColumn::Selector buildSelector(int num_rows, int num_columns)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, num_columns - 1);

    IColumn::Selector selector;
    for (int i = 0; i < num_rows; ++i)
        selector.push_back(dist(mt));
    return selector;
}

template <typename T>
void testScatter(int num_rows, int num_columns, int seconds)
{
    ColumnPtr src = buildColumn<T>(num_rows);
    auto selector = buildSelector(num_rows, num_columns);

    StopFlag stop_flag = false;
    std::atomic<Int64> counter = 0;

    auto start = std::chrono::high_resolution_clock::now();

    auto scatter_func = [&] {
        while (!stop_flag.load(std::memory_order_relaxed))
        {
            src->scatter(num_columns, selector);
            counter.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::thread t(scatter_func);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);
    t.join();

    auto cur = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - start);
    std::cout << fmt::format("Scatter/s: {:<10}", counter.load() * 1000 / duration.count()) << std::endl;
}

} // namespace
} // namespace DB::tests

int main(int argc [[maybe_unused]], char ** argv [[maybe_unused]])
{
    if (argc < 2 || argc > 6)
    {
        std::cerr << fmt::format("Usage: {} [int|int64] [scatter] <rows=10000> <columns=5> <seconds=10>", argv[0])
                  << std::endl;
        exit(1);
    }

    using TestHandler = std::function<void(int rows, int columns, int seconds)>;
    static const std::unordered_map<String, std::unordered_map<String, TestHandler>> handlers
        = {{"int", {{"scatter", DB::tests::testScatter<Int32>}}},
           {"int64", {{"scatter", DB::tests::testScatter<Int64>}}}};

    String type_name = argv[1];
    String method = argv[2];
    int rows = argc >= 4 ? atoi(argv[3]) : 10000;
    int columns = argc >= 5 ? atoi(argv[4]) : 5;
    int seconds = argc >= 6 ? atoi(argv[5]) : 10;

    const auto & find_handler = [](const String & title, const String & name, const auto & handler_map) {
        auto it = handler_map.find(name);
        if (it == end(handler_map))
        {
            std::cerr << fmt::format("Unknown {}: {}", title, name) << std::endl;
            exit(1);
        }
        return it->second;
    };

    auto handler = find_handler("method", method, find_handler("type", type_name, handlers));

    std::cout << fmt::format("Test {}-{} rows={} columns={} seconds={}", type_name, method, rows, columns, seconds)
              << std::endl;
    handler(rows, columns, seconds);
}

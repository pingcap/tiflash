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
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <common/types.h>
#include <fmt/core.h>

#include <atomic>
#include <chrono>
#include <random>
#include <thread>

namespace DB::tests
{
namespace
{
std::random_device rd;

using StopFlag = std::atomic<bool>;

ColumnPtr filterV1(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    const auto & data = typeid_cast<const ColumnVector<Int64> *>(col.get())->getData();
    size_t size = col->size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnVector<Int64>::create();
    using Container = ColumnVector<Int64>::Container;
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const Int64 * data_pos = &data[0];

#if __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse)
    {
        int mask
            = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
        }
        else if (0xFFFF == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            for (size_t i = 0; i < SIMD_BYTES; ++i)
                if (filt_pos[i])
                    res_data.push_back(data_pos[i]);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
#endif

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

ColumnPtr filterV2(ColumnPtr & col, IColumn::Filter & filt, ssize_t result_size_hint)
{
    const auto & data = typeid_cast<const ColumnVector<Int64> *>(col.get())->getData();
    size_t size = col->size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnVector<Int64>::create();
    using Container = ColumnVector<Int64>::Container;
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const Int64 * data_pos = &data[0];

    filterImpl(filt_pos, filt_end, data_pos, res_data);

    return res;
}

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

IColumn::Filter buildFilter(int num_rows, int num_columns)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(1, 100);

    IColumn::Filter filter;
    filter.resize(num_rows);
    for (int i = 0; i < num_rows; ++i)
        filter[i] = dist(mt) % num_columns == 0;
    return filter;
}

template <typename T>
void testFilter(int num_rows, int num_columns, int seconds)
{
    ColumnPtr src = buildColumn<T>(num_rows);
    auto filter = buildFilter(num_rows, num_columns);

    StopFlag stop_flag = false;
    std::vector<std::atomic<Int64>> counters(2);
    for (auto & counter : counters)
        counter = 0;

    const std::vector<std::function<void()>> filter_func
        = {[&] {
               while (!stop_flag.load(std::memory_order_relaxed))
               {
                   filterV1(src, filter, -1);
                   counters[0].fetch_add(1, std::memory_order_relaxed);
               }
           },
           [&] {
               while (!stop_flag.load(std::memory_order_relaxed))
               {
                   filterV2(src, filter, -1);
                   counters[1].fetch_add(1, std::memory_order_relaxed);
               }
           }};

    std::vector<std::thread> threads;
    threads.reserve(filter_func.size());
    auto start = std::chrono::high_resolution_clock::now();
    for (const auto & f : filter_func)
    {
        threads.emplace_back(f);
    }

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);
    for (auto & t : threads)
        t.join();

    auto cur = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - start);
    for (size_t i = 0; i < counters.size(); ++i)
    {
        std::cout << fmt::format("FilterV{}: {:<10}", i + 1, counters[i].load() * 1000 / duration.count()) << std::endl;
    }
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
        = {{"int", {{"scatter", DB::tests::testScatter<Int32>}, {"filter", DB::tests::testFilter<Int64>}}},
           {"int64", {{"scatter", DB::tests::testScatter<Int64>}, {"filter", DB::tests::testFilter<Int64>}}}};

    String type_name = argv[1];
    String method = argv[2];
    int rows = argc >= 4 ? std::stoi(argv[3]) : 10000;
    int columns = argc >= 5 ? std::stoi(argv[4]) : 5;
    int seconds = argc >= 6 ? std::stoi(argv[5]) : 10;

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

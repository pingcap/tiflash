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

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/MPMCQueue.h>
#include <common/types.h>
#include <fmt/core.h>

#include <memory>
#include <random>
#include <thread>
#include <unordered_map>

namespace DB::tests
{
namespace
{
std::random_device rd;
std::mt19937 mt(rd());

struct ShortString
{
    String str;
};

struct LongString
{
    String str;
};

String generateString(int min_len, int max_len)
{
    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<std::int8_t> char_dist;
    String str;
    int len = len_dist(mt);
    for (int i = 0; i < len; ++i)
        str.push_back(char_dist(mt));
    return str;
}

void fillData(std::vector<int> & data, int n)
{
    std::uniform_int_distribution<int> dist;
    data.clear();
    for (int i = 0; i < n; ++i)
        data.push_back(dist(mt));
}

void fillData(std::vector<ShortString> & data, int n)
{
    data.clear();
    for (int i = 0; i < n; ++i)
        data.push_back({generateString(5, 8)});
}

void fillData(std::vector<LongString> & data, int n)
{
    data.clear();
    for (int i = 0; i < n; ++i)
        data.push_back({generateString(50, 60)});
}

template <typename T>
struct Helper;

template <typename T>
struct Helper<MPMCQueue<T>>
{
    static void popOneFrom(MPMCQueue<T> & queue)
    {
        T t;
        queue.pop(t);
    }

    template <typename U>
    static void pushOneTo(MPMCQueue<T> & queue, U && data)
    {
        queue.pushTimeout(std::forward<U>(data), std::chrono::milliseconds(1));
    }
};

template <typename T>
struct Helper<ConcurrentBoundedQueue<T>>
{
    static void popOneFrom(ConcurrentBoundedQueue<T> & queue)
    {
        T t;
        queue.pop(t);
    }

    template <typename U>
    static void pushOneTo(ConcurrentBoundedQueue<T> & queue, U && data)
    {
        queue.tryPush(std::forward<U>(data), 1);
    }
};

template <template <typename> typename QueueType, typename ValueType>
void test(
    int capacity [[maybe_unused]],
    int reader_cnt [[maybe_unused]],
    int writer_cnt [[maybe_unused]],
    int seconds [[maybe_unused]])
{
    QueueType<ValueType> queue(capacity);
    std::vector<ValueType> data;
    fillData(data, 10000);

    std::uniform_int_distribution<int> dist;

    std::atomic<bool> read_stop = false;
    std::atomic<bool> write_stop = false;
    std::atomic<Int64> counter = 0;

    auto read_func = [&] {
        while (!read_stop.load(std::memory_order_relaxed))
        {
            for (int i = 0; i < 10; ++i)
                Helper<QueueType<ValueType>>::popOneFrom(queue);
            counter.fetch_add(10, std::memory_order_acq_rel);
        }
    };

    auto write_func = [&] {
        while (!write_stop.load(std::memory_order_relaxed))
            for (int i = 0; i < 10; ++i)
                Helper<QueueType<ValueType>>::pushOneTo(queue, data[dist(mt) % data.size()]);
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < reader_cnt; ++i)
        readers.emplace_back(read_func);

    std::vector<std::thread> writers;
    for (int i = 0; i < writer_cnt; ++i)
        writers.emplace_back(write_func);

    auto start = std::chrono::high_resolution_clock::now();
    auto deadline = start + std::chrono::seconds(seconds);
    auto second_ago = start;
    Int64 last_counter = counter.load(std::memory_order_acquire);
    while (true)
    {
        auto cur = std::chrono::high_resolution_clock::now();
        if (cur >= deadline)
            break;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - second_ago);
        if (duration.count() >= 1000)
        {
            Int64 cur_counter = counter.load(std::memory_order_acquire);
            std::cout << fmt::format(
                "Count: {:<14} Count/s: {:<14}",
                cur_counter,
                (cur_counter - last_counter) * 1000 / duration.count())
                      << std::endl;
            second_ago = cur;
            last_counter = cur_counter;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    read_stop.store(true);
    for (auto & t : readers)
        t.join();
    write_stop.store(true);
    for (auto & t : writers)
        t.join();

    auto cur = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - start);
    Int64 cur_counter = counter.load(std::memory_order_acquire);
    std::cout << fmt::format("End. Count: {:<14} Count/s: {:<14}", cur_counter, cur_counter * 1000 / duration.count())
              << std::endl;
}

} // namespace
} // namespace DB::tests

int main(int argc, char ** argv)
{
    if (argc < 3 || argc > 7)
    {
        auto usage = fmt::format(
            "Usage: {} [MPMCQueue|ConcurrentBoundedQueue] [Int|ShortString|LongString] <capacity=4> <reader=4> "
            "<writer=4> <seconds=10>",
            argv[0]);
        std::cerr << usage << std::endl;
        exit(1);
    }

    String class_name = argv[1];
    String type_name = argv[2];
    int capacity = argc >= 4 ? atoi(argv[3]) : 4;
    int reader_cnt = argc >= 5 ? atoi(argv[4]) : 4;
    int writer_cnt = argc >= 6 ? atoi(argv[5]) : 4;
    int seconds = argc >= 7 ? atoi(argv[6]) : 10;

    using TestHandler = std::function<void(int capacity, int reader_cnt, int writer_cnt, int seconds)>;
    static const std::unordered_map<String, std::unordered_map<String, TestHandler>> handlers
        = {{"MPMCQueue",
            {{"Int", DB::tests::test<DB::MPMCQueue, int>},
             {"ShortString", DB::tests::test<DB::MPMCQueue, DB::tests::ShortString>},
             {"LongString", DB::tests::test<DB::MPMCQueue, DB::tests::LongString>}}},
           {"ConcurrentBoundedQueue",
            {{"Int", DB::tests::test<ConcurrentBoundedQueue, int>},
             {"ShortString", DB::tests::test<ConcurrentBoundedQueue, DB::tests::ShortString>},
             {"LongString", DB::tests::test<ConcurrentBoundedQueue, DB::tests::LongString>}}}};

    const auto & find_handler = [](const String & title, const String & name, const auto & handler_map) {
        auto it = handler_map.find(name);
        if (it == end(handler_map))
        {
            std::cerr << fmt::format("Unknown {}: {}", title, name) << std::endl;
            exit(1);
        }
        return it->second;
    };

    auto handler = find_handler("type", type_name, find_handler("class", class_name, handlers));

    std::cout << fmt::format(
        "Run MPMC test for {}-{} capacity={} readers={} writers={} seconds={}",
        class_name,
        type_name,
        capacity,
        reader_cnt,
        writer_cnt,
        seconds)
              << std::endl;
    handler(capacity, reader_cnt, writer_cnt, seconds);
}

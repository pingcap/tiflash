// Copyright 2022 PingCAP, Ltd.
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

#include <Common/ConcurrentIOQueue.h>
#include <Common/ConcurrentIOQueue2.h>
#include <Common/ThreadManager.h>

#include <benchmark/benchmark.h>

namespace DB
{
namespace tests
{
class IOQueueBench : public benchmark::Fixture
{
};

BENCHMARK_DEFINE_F(IOQueueBench, is_full)
(benchmark::State & state)
try
{
    const int concurrency = state.range(0);
    const int round = state.range(1);
    const bool is_new = state.range(2);

    if (is_new)
    {
        ConcurrentIOQueue io_queue{1};
        for (size_t i = 0; i < concurrency; ++i)
        {
            thread_manager->schedule(false, "concurrency", [&]() {
                for (size_t i = 0; i < round; ++i)
                    io_queue.isFull();
            });
        }
        thread_manager->wait();
    }
    else
    {
        ConcurrentIOQueue2 io_queue{1};
        for (size_t i = 0; i < concurrency; ++i)
        {
            thread_manager->schedule(false, "concurrency", [&]() {
                for (size_t i = 0; i < round; ++i)
                    io_queue.isFull();
            });
        }
        thread_manager->wait();
    }
}
CATCH
BENCHMARK_REGISTER_F(IOQueueBench, is_full)
    ->Args({1, 999999, false})
    ->Args({1, 999999, true})
    ->Args({2, 999999, false})
    ->Args({2, 999999, true})
    ->Args({8, 999999, false})
    ->Args({8, 999999, true});

BENCHMARK_DEFINE_F(IOQueueBench, try_pop)
(benchmark::State & state)
try
{
    const int concurrency = state.range(0);
    const int round = state.range(1);
    const bool is_new = state.range(2);

    if (is_new)
    {
        ConcurrentIOQueue io_queue{1};
        for (size_t i = 0; i < concurrency; ++i)
        {
            thread_manager->schedule(false, "concurrency", [&]() {
                for (size_t i = 0; i < round; ++i)
                    io_queue.tryPop();
            });
        }
        thread_manager->wait();
    }
    else
    {
        ConcurrentIOQueue2 io_queue{1};
        for (size_t i = 0; i < concurrency; ++i)
        {
            thread_manager->schedule(false, "concurrency", [&]() {
                for (size_t i = 0; i < round; ++i)
                    io_queue.tryPop();
            });
        }
        thread_manager->wait();
    }
}
CATCH
BENCHMARK_REGISTER_F(IOQueueBench, try_pop)
    ->Args({1, 999999, false})
    ->Args({1, 999999, true})
    ->Args({2, 999999, false})
    ->Args({2, 999999, true})
    ->Args({8, 999999, false})
    ->Args({8, 999999, true});

} // namespace tests
} // namespace DB

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
#include <common/mpmcstack.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <thread>
namespace common
{
struct StackCell
{
    std::atomic<StackCell *> next{nullptr};
    int value;
    explicit StackCell(int value)
        : value(value)
    {}
};
TEST(MPMCStackTest, SingleThreadGeneric)
{
    MPMCStack<StackCell> stack(true);
    std::vector<int> value;
    int sum = 0;
    int acc = 0;
    for (int i = 0; i < 100000; ++i)
    {
        sum += i;
        value.push_back(i);
    }
    std::shuffle(value.begin(), value.end(), std::default_random_engine(std::random_device()()));
    for (auto i : value)
    {
        stack.push(new StackCell{i});
    }
    while (auto * res = stack.pop())
    {
        acc += res->value;
        delete res;
    }
    EXPECT_EQ(sum, acc);
}
TEST(MPMCStackTest, SingleThreadXchg)
{
    MPMCStack<StackCell> stack(false);
    std::vector<int> value;
    int sum = 0;
    int acc = 0;
    for (int i = 0; i < 100000; ++i)
    {
        sum += i;
        value.push_back(i);
    }
    std::shuffle(value.begin(), value.end(), std::default_random_engine(std::random_device()()));
    for (auto i : value)
    {
        stack.push(new StackCell{i});
    }
    while (auto * res = stack.pop())
    {
        acc += res->value;
        delete res;
    }
    EXPECT_EQ(sum, acc);
}

TEST(MPMCStackTest, MultiThreadGeneric)
{
    MPMCStack<StackCell> stack(true);
    std::atomic_flag finished{};
    std::atomic_size_t acc{0};
    std::vector<int> value;
    std::vector<std::thread> consumers;
    std::vector<std::thread> producers;

    size_t sum = 0;
    for (size_t i = 0; i < 10000; ++i)
    {
        sum += i;
        value.push_back(i);
    }
    std::shuffle(value.begin(), value.end(), std::default_random_engine(std::random_device()()));

    for (int i = 0; i < 10; ++i)
    {
        consumers.emplace_back([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
            while (true)
            {
                auto * res = stack.pop();
                if (!res && finished.test())
                {
                    break;
                }
                if (res)
                {
                    acc += res->value;
                    delete res;
                }
                std::this_thread::yield();
            }
        });
    }

    for (int i = 0; i < 10; ++i)
    {
        producers.emplace_back([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            for (auto i : value)
            {
                stack.push(new StackCell{i});
                std::this_thread::yield();
            }
        });
    }

    for (auto & i : producers)
    {
        i.join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    finished.test_and_set();

    for (auto & i : consumers)
    {
        i.join();
    }

    EXPECT_EQ(producers.size() * sum, acc.load());
}

TEST(MPMCStackTest, MultiThreadXchg)
{
    MPMCStack<StackCell> stack(false);
    std::atomic_flag finished{};
    std::atomic_size_t acc{0};
    std::vector<int> value;
    std::vector<std::thread> consumers;
    std::vector<std::thread> producers;

    size_t sum = 0;
    for (size_t i = 0; i < 10000; ++i)
    {
        sum += i;
        value.push_back(i);
    }
    std::shuffle(value.begin(), value.end(), std::default_random_engine(std::random_device()()));

    for (int i = 0; i < 10; ++i)
    {
        consumers.emplace_back([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
            while (true)
            {
                auto * res = stack.pop();
                if (!res && finished.test())
                {
                    break;
                }
                if (res)
                {
                    acc += res->value;
                    delete res;
                }
                std::this_thread::yield();
            }
        });
    }

    for (int i = 0; i < 10; ++i)
    {
        producers.emplace_back([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            for (auto i : value)
            {
                stack.push(new StackCell{i});
                std::this_thread::yield();
            }
        });
    }

    for (auto & i : producers)
    {
        i.join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    finished.test_and_set();

    for (auto & i : consumers)
    {
        i.join();
    }

    EXPECT_EQ(producers.size() * sum, acc.load());
}

} // namespace common
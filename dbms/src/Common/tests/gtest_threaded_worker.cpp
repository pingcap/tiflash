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

#include <Common/MPMCQueue.h>
#include <Common/ThreadedWorker.h>
#include <gtest/gtest.h>

#include <thread>

using namespace std::chrono_literals;

namespace DB
{
namespace tests
{

TEST(ThreadedWorker, Finish)
{
    class WorkerMultiply : public ThreadedWorker<Int64, Int64>
    {
    public:
        using ThreadedWorker<Int64, Int64>::ThreadedWorker;

    protected:
        Int64 doWork(const Int64 & value) override { return value * 2; }

        String getName() const noexcept override { return "WorkerMultiply"; }
    };

    auto w = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(5),
        /* result */ std::make_shared<MPMCQueue<Int64>>(5),
        Logger::get(),
        1);

    Int64 result = 0;
    auto pop_result = w.result_queue->popTimeout(result, 100ms);
    ASSERT_EQ(pop_result, MPMCQueueResult::TIMEOUT);

    // When worker is not started, push should not trigger any results to be produced.
    w.source_queue->push(1);
    w.source_queue->push(3);
    w.source_queue->push(5);

    pop_result = w.result_queue->popTimeout(result, 300ms);
    ASSERT_EQ(pop_result, MPMCQueueResult::TIMEOUT);

    w.startInBackground();

    w.source_queue->push(7);

    w.result_queue->pop(result);
    ASSERT_EQ(result, 2);

    w.result_queue->pop(result);
    ASSERT_EQ(result, 6);

    w.result_queue->pop(result);
    ASSERT_EQ(result, 10);

    w.result_queue->pop(result);
    ASSERT_EQ(result, 14);

    pop_result = w.result_queue->popTimeout(result, 300ms);
    ASSERT_EQ(pop_result, MPMCQueueResult::TIMEOUT);

    // When source queue is finished, result queue should be finished.
    w.source_queue->finish();

    pop_result = w.result_queue->pop(result);
    ASSERT_EQ(pop_result, MPMCQueueResult::FINISHED);
}


TEST(ThreadedWorker, Finish2)
{
    using Result = std::pair<Int64, Int64>;

    class WorkerMultiply : public ThreadedWorker<Int64, Result>
    {
    public:
        using ThreadedWorker<Int64, Result>::ThreadedWorker;

    protected:
        Result doWork(const Int64 & value) override
        {
            std::this_thread::sleep_for(100ms);
            return {value, value * 2};
        }

        String getName() const noexcept override { return "WorkerMultiply"; }
    };

    auto w = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(100),
        /* result */ std::make_shared<MPMCQueue<Result>>(100),
        Logger::get(),
        10);

    std::set<Int64> remainings;
    for (int i = 0; i < 5; i++)
    {
        remainings.emplace(i);
        w.source_queue->push(i);
    }
    w.source_queue->finish();

    w.startInBackground();

    Result r;
    for (int i = 0; i < 5; i++)
    {
        w.result_queue->pop(r);

        ASSERT_TRUE(remainings.contains(r.first));
        remainings.erase(r.first);
        ASSERT_EQ(r.first * 2, r.second);
    }
    ASSERT_TRUE(remainings.empty());

    auto pop_result = w.result_queue->pop(r);
    ASSERT_EQ(pop_result, MPMCQueueResult::FINISHED);
}


} // namespace tests
} // namespace DB

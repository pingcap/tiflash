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

#include <Common/MPMCQueue.h>
#include <Common/ThreadedWorker.h>
#include <gtest/gtest.h>

#include <thread>

using namespace std::chrono_literals;

namespace DB
{
namespace tests
{

class WorkerMultiply : public ThreadedWorker<Int64, Int64>
{
public:
    using ThreadedWorker<Int64, Int64>::ThreadedWorker;

    explicit WorkerMultiply(
        std::shared_ptr<MPMCQueue<Int64>> source_queue_,
        std::shared_ptr<MPMCQueue<Int64>> result_queue_,
        size_t concurrency,
        Int64 multiply_factor_ = 2,
        std::chrono::duration<double> sleep_duration_ = 50ms)
        : ThreadedWorker(source_queue_, result_queue_, Logger::get(), concurrency)
        , multiply_factor(multiply_factor_)
        , sleep_duration(sleep_duration_)
    {}

    ~WorkerMultiply() override { wait(); }

protected:
    Int64 doWork(const Int64 & value) override
    {
        std::this_thread::sleep_for(sleep_duration);
        return value * multiply_factor;
    }

    String getName() const noexcept override { return "Multiply"; }

private:
    Int64 multiply_factor = 2;
    std::chrono::duration<double> sleep_duration = 50ms;
};

class WorkerErrorAtN : public ThreadedWorker<Int64, Int64>
{
public:
    using ThreadedWorker<Int64, Int64>::ThreadedWorker;

    explicit WorkerErrorAtN(
        std::shared_ptr<MPMCQueue<Int64>> source_queue_,
        std::shared_ptr<MPMCQueue<Int64>> result_queue_,
        size_t concurrency,
        Int64 error_at_,
        std::chrono::duration<double> sleep_duration_ = 50ms)
        : ThreadedWorker(source_queue_, result_queue_, Logger::get(), concurrency)
        , error_at(error_at_)
        , sleep_duration(sleep_duration_)
    {}

    ~WorkerErrorAtN() override { wait(); }

protected:
    Int64 doWork(const Int64 & value) override
    {
        auto n = current_n.fetch_add(1);
        std::this_thread::sleep_for(sleep_duration);

        if (static_cast<size_t>(n) == error_at)
            throw Exception("Error");

        return value;
    }

    String getName() const noexcept override { return "ErrorAtN"; }

private:
    size_t error_at = 0;
    std::atomic<Int64> current_n = 0;
    std::chrono::duration<double> sleep_duration = 50ms;
};

TEST(ThreadedWorker, SingleThread)
{
    auto w = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(5),
        /* result */ std::make_shared<MPMCQueue<Int64>>(5),
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

TEST(ThreadedWorker, CancelResultQueue)
{
    auto w = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(5),
        /* result */ std::make_shared<MPMCQueue<Int64>>(5),
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

    w.result_queue->cancelWith("testing"); // cancel the result queue

    w.startInBackground();

    pop_result = w.result_queue->pop(result);
    ASSERT_EQ(pop_result, MPMCQueueResult::CANCELLED);

    w.wait(); // finish normally
}

TEST(ThreadedWorker, MultiThread)
{
    auto w = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(100),
        /* result */ std::make_shared<MPMCQueue<Int64>>(100),
        10);

    std::multiset<Int64> remainings_results;
    for (Int64 v : {23, 13, 41, 17, 19})
    {
        remainings_results.emplace(v * 2);
        w.source_queue->push(v);
    }

    w.startInBackground();

    Int64 r = 0;
    for (int i = 0; i < 5; i++)
    {
        w.result_queue->pop(r);
        ASSERT_TRUE(remainings_results.contains(r));
        remainings_results.erase(remainings_results.find(r));
    }
    ASSERT_TRUE(remainings_results.empty());

    w.source_queue->push(2);
    w.result_queue->pop(r);
    ASSERT_EQ(r, 4);

    w.source_queue->push(3);
    w.result_queue->pop(r);
    ASSERT_EQ(r, 6);

    w.source_queue->finish();
    auto pop_result = w.result_queue->pop(r);
    ASSERT_EQ(pop_result, MPMCQueueResult::FINISHED);
}

TEST(ThreadedWorker, Chainned)
{
    auto w1 = WorkerMultiply(
        /* src */ std::make_shared<MPMCQueue<Int64>>(100),
        /* result */ std::make_shared<MPMCQueue<Int64>>(100),
        2);

    auto w2 = WorkerMultiply(
        /* src */ w1.result_queue,
        /* result */ std::make_shared<MPMCQueue<Int64>>(100),
        10,
        /* multiply_factor*/ 3);

    std::multiset<Int64> remainings_results;
    for (Int64 v : {5, 7, 43, 47, 5, 7})
    {
        remainings_results.emplace(v * 2 * 3);
        w1.source_queue->push(v);
    }
    w1.source_queue->finish();

    w1.startInBackground();
    w2.startInBackground();

    Int64 r = 0;
    for (int i = 0; i < 6; i++)
    {
        w2.result_queue->pop(r);
        ASSERT_TRUE(remainings_results.contains(r));
        remainings_results.erase(remainings_results.find(r));
    }
    ASSERT_TRUE(remainings_results.empty());

    auto pop_result = w2.result_queue->pop(r);
    ASSERT_EQ(pop_result, MPMCQueueResult::FINISHED);
}

TEST(ThreadedWorker, ErrorInWorker)
{
    auto w = WorkerErrorAtN(
        /* src */ std::make_shared<MPMCQueue<Int64>>(100),
        /* result */ std::make_shared<MPMCQueue<Int64>>(100),
        2,
        /* error_at */ 3);

    w.startInBackground();

    Int64 r = 0;

    w.source_queue->push(4);
    w.result_queue->pop(r);
    ASSERT_EQ(r, 4);

    w.source_queue->push(3);
    w.result_queue->pop(r);
    ASSERT_EQ(r, 3);

    w.source_queue->push(7);
    w.result_queue->pop(r);
    ASSERT_EQ(r, 7);

    w.source_queue->push(1);
    auto result = w.result_queue->pop(r);
    ASSERT_EQ(result, MPMCQueueResult::CANCELLED);

    result = w.source_queue->push(4);
    ASSERT_EQ(result, MPMCQueueResult::CANCELLED);
}

TEST(ThreadedWorker, ErrorInWorkerWithNonEmptyQueue)
{
    auto w = WorkerErrorAtN(
        /* src */ std::make_shared<MPMCQueue<Int64>>(100),
        /* result */ std::make_shared<MPMCQueue<Int64>>(100),
        2,
        /* error_at */ 3);

    w.source_queue->push(5);
    w.source_queue->push(1);
    w.source_queue->push(3);
    w.source_queue->push(13);
    w.source_queue->push(7);
    w.source_queue->push(11);

    w.startInBackground();

    Int64 r = 0;
    bool error_happened = false;
    for (size_t i = 0; i < 6; ++i)
    {
        auto result = w.result_queue->pop(r);
        if (result == MPMCQueueResult::CANCELLED)
        {
            error_happened = true;
            break;
        }
    }
    ASSERT_TRUE(error_happened);

    auto result = w.source_queue->push(10);
    ASSERT_EQ(result, MPMCQueueResult::CANCELLED);
}


} // namespace tests
} // namespace DB

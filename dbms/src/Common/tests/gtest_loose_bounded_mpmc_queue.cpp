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

#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/ThreadManager.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <atomic>
#include <thread>

namespace DB::tests
{
namespace
{
class LooseBoundedMPMCQueueTest : public ::testing::Test
{
};

TEST_F(LooseBoundedMPMCQueueTest, base)
try
{
    // case 1 normal
    {
        size_t max_size = 10;
        LooseBoundedMPMCQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(queue.isWritable());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ(queue.forcePush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ((max_size + 1), queue.size());

        for (size_t i = 0; i < max_size; ++i)
            ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isWritable());
        size_t i;
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        ASSERT_EQ(queue.tryPop(i), MPMCQueueResult::EMPTY);
    }

    // case 2 finished
    {
        size_t max_size = 10;
        LooseBoundedMPMCQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(queue.isWritable());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ(queue.forcePush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ((max_size + 1), queue.size());

        queue.finish();
        ASSERT_TRUE(queue.isWritable());
        ASSERT_EQ(queue.push(0 + max_size), MPMCQueueResult::FINISHED);
        for (size_t i = 0; i < max_size; ++i)
            ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        size_t i;
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::FINISHED);
        ASSERT_EQ(queue.tryPop(i), MPMCQueueResult::FINISHED);
    }

    // case 3 cancelled
    {
        size_t max_size = 10;
        LooseBoundedMPMCQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(queue.isWritable());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ(queue.forcePush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isWritable());
        ASSERT_EQ((max_size + 1), queue.size());

        queue.cancel();
        ASSERT_TRUE(queue.isWritable());
        ASSERT_EQ(queue.push(0 + max_size), MPMCQueueResult::CANCELLED);
        size_t i;
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::CANCELLED);
        ASSERT_EQ(queue.tryPop(i), MPMCQueueResult::CANCELLED);
    }
}
CATCH

TEST_F(LooseBoundedMPMCQueueTest, ConcurrentForcePushAndPop)
try
{
    for (size_t producer = 1; producer < 25; producer += 4)
    {
        for (size_t consumer = 1; consumer < 25; consumer += 4)
        {
            for (size_t queue_size = 1; queue_size < 25; queue_size += 4)
            {
                auto thread_manager = newThreadManager();
                LooseBoundedMPMCQueue<Int64> queue(queue_size);

                std::atomic_size_t producer_num = producer;
                size_t produce_count_per_producer = 1000;
                std::atomic_size_t total_count = producer_num * produce_count_per_producer;
                for (size_t i = 0; i < producer_num; ++i)
                {
                    thread_manager->schedule(false, "producer", [&]() {
                        for (size_t i = 0; i < produce_count_per_producer; ++i)
                        {
                            while (!queue.isWritable()) {}
                            Int64 tmp = 0;
                            ASSERT_EQ(queue.forcePush(std::move(tmp)), MPMCQueueResult::OK);
                        }
                        if (1 == producer_num.fetch_sub(1))
                            queue.finish();
                    });
                }
                for (size_t i = 0; i < consumer; ++i)
                {
                    thread_manager->schedule(false, "consumer", [&]() {
                        while (true)
                        {
                            Int64 tmp;
                            if (queue.pop(tmp) == MPMCQueueResult::OK)
                                --total_count;
                            else
                                return;
                        }
                    });
                }
                thread_manager->wait();
                ASSERT_EQ(0, total_count);
            }
        }
    }
}
CATCH

TEST_F(LooseBoundedMPMCQueueTest, ConcurrentForcePushAndTryPop)
try
{
    for (size_t producer = 1; producer < 25; producer += 4)
    {
        for (size_t consumer = 1; consumer < 25; consumer += 4)
        {
            for (size_t queue_size = 1; queue_size < 25; queue_size += 4)
            {
                auto thread_manager = newThreadManager();
                LooseBoundedMPMCQueue<Int64> queue(queue_size);

                std::atomic_size_t producer_num = producer;
                size_t produce_count_per_producer = 1000;
                std::atomic_size_t total_count = producer_num * produce_count_per_producer;
                for (size_t i = 0; i < producer_num; ++i)
                {
                    thread_manager->schedule(false, "producer", [&]() {
                        for (size_t i = 0; i < produce_count_per_producer; ++i)
                        {
                            while (!queue.isWritable()) {}
                            Int64 tmp = 0;
                            ASSERT_EQ(queue.forcePush(std::move(tmp)), MPMCQueueResult::OK);
                        }
                        if (1 == producer_num.fetch_sub(1))
                            queue.finish();
                    });
                }
                for (size_t i = 0; i < consumer; ++i)
                {
                    thread_manager->schedule(false, "consumer", [&]() {
                        while (true)
                        {
                            Int64 tmp;
                            switch (queue.tryPop(tmp))
                            {
                            case MPMCQueueResult::OK:
                                --total_count;
                            case MPMCQueueResult::EMPTY:
                                break;
                            default:
                                return;
                            }
                        }
                    });
                }
                thread_manager->wait();
                ASSERT_EQ(0, total_count);
            }
        }
    }
}
CATCH

TEST_F(LooseBoundedMPMCQueueTest, AuxiliaryMemoryBound)
try
{
    size_t max_size = 10;
    Int64 auxiliary_memory_bound;
    Int64 value;

    {
        /// case 1: no auxiliary memory usage bound
        LooseBoundedMPMCQueue<Int64> queue(max_size);
        for (size_t i = 0; i < max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
    }

    {
        /// case 2: less auxiliary memory bound than the capacity bound
        size_t actual_max_size = 5;
        auxiliary_memory_bound = sizeof(Int64) * actual_max_size;
        LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, auxiliary_memory_bound), [](const Int64 &) {
            return sizeof(Int64);
        });
        for (size_t i = 0; i < actual_max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(actual_max_size) == MPMCQueueResult::FULL);
        /// after pop one element, the queue can be pushed again
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(actual_max_size) == MPMCQueueResult::OK);
    }

    {
        /// case 3: less capacity bound than the auxiliary memory bound
        auxiliary_memory_bound = sizeof(Int64) * (max_size * 10);
        LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, auxiliary_memory_bound), [](const Int64 &) {
            return sizeof(Int64);
        });
        for (size_t i = 0; i < max_size; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
    }

    {
        /// case 4, auxiliary memory bound <= 0 means unbounded for auxiliary memory usage
        std::vector<Int64> bounds{0, -1};
        for (const auto & bound : bounds)
        {
            LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, bound), [](const Int64 &) {
                return 1024 * 1024;
            });
            for (size_t i = 0; i < max_size; i++)
                ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
            ASSERT_TRUE(queue.tryPush(max_size) == MPMCQueueResult::FULL);
        }
    }

    {
        /// case 5 even if the element's auxiliary memory is out of bound, at least one element can be pushed
        LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, 1), [](const Int64 &) { return 10; });
        ASSERT_TRUE(queue.tryPush(1) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(2) == MPMCQueueResult::FULL);
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPop(value) == MPMCQueueResult::EMPTY);
        ASSERT_TRUE(queue.tryPush(1) == MPMCQueueResult::OK);
    }

    {
        /// case 6 after pop a huge element, more than one small push can be notified without further pop
        LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, 20), [](const Int64 & element) {
            return std::abs(element);
        });
        ASSERT_TRUE(queue.tryPush(100) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPush(5) == MPMCQueueResult::FULL);
        auto thread_manager = newThreadManager();
        thread_manager->schedule(false, "thread_1", [&]() { queue.push(5); });
        thread_manager->schedule(false, "thread_2", [&]() { queue.push(6); });
        std::exception_ptr current_exception = nullptr;
        try
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            ASSERT_TRUE(queue.pop(value) == MPMCQueueResult::OK);
            ASSERT_EQ(value, 100);
            std::this_thread::sleep_for(std::chrono::seconds(5));
            ASSERT_EQ(queue.size(), 2);
        }
        catch (...)
        {
            current_exception = std::current_exception();
            queue.cancelWith("test failed");
        }
        thread_manager->wait();
        if (current_exception)
            std::rethrow_exception(current_exception);
    }

    {
        /// case 7, force push does not limited by memory bound
        LooseBoundedMPMCQueue<Int64> queue(CapacityLimits(max_size, sizeof(Int64) * max_size / 2), [](const Int64 &) {
            return sizeof(Int64);
        });
        for (size_t i = 0; i < max_size / 2; i++)
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::OK);
        for (size_t i = max_size / 2; i < max_size; i++)
        {
            ASSERT_TRUE(queue.tryPush(i) == MPMCQueueResult::FULL);
            ASSERT_TRUE(queue.forcePush(i) == MPMCQueueResult::OK);
        }
    }
}
CATCH

} // namespace
} // namespace DB::tests

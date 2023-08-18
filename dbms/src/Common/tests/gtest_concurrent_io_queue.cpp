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

#include <Common/ConcurrentIOQueue.h>
#include <Common/ThreadManager.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <atomic>

namespace DB::tests
{
namespace
{
class ConcurrentIOQueueTest : public ::testing::Test
{
};

TEST_F(ConcurrentIOQueueTest, base)
try
{
    // case 1 normal
    {
        size_t max_size = 10;
        ConcurrentIOQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ(queue.nonBlockingPush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ((max_size + 1), queue.size());

        for (size_t i = 0; i < max_size; ++i)
            ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isFull());
        size_t i;
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::OK);
        ASSERT_EQ(queue.tryPop(i), MPMCQueueResult::EMPTY);
    }

    // case 2 finished
    {
        size_t max_size = 10;
        ConcurrentIOQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ(queue.nonBlockingPush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ((max_size + 1), queue.size());

        queue.finish();
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
        ConcurrentIOQueue<size_t> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_EQ(queue.push(std::move(i)), MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ(queue.nonBlockingPush(0 + max_size), MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_EQ((max_size + 1), queue.size());

        queue.cancel();
        ASSERT_EQ(queue.push(0 + max_size), MPMCQueueResult::CANCELLED);
        size_t i;
        ASSERT_EQ(queue.pop(i), MPMCQueueResult::CANCELLED);
        ASSERT_EQ(queue.tryPop(i), MPMCQueueResult::CANCELLED);
    }
}
CATCH

TEST_F(ConcurrentIOQueueTest, ConcurrentNonBlockingPushAndPop)
try
{
    for (size_t producer = 1; producer < 25; producer += 4)
    {
        for (size_t consumer = 1; consumer < 25; consumer += 4)
        {
            for (size_t queue_size = 1; queue_size < 25; queue_size += 4)
            {
                auto thread_manager = newThreadManager();
                ConcurrentIOQueue<Int64> queue(queue_size);

                std::atomic_size_t producer_num = producer;
                size_t produce_count_per_producer = 1000;
                std::atomic_size_t total_count = producer_num * produce_count_per_producer;
                for (size_t i = 0; i < producer_num; ++i)
                {
                    thread_manager->schedule(false, "producer", [&]() {
                        for (size_t i = 0; i < produce_count_per_producer; ++i)
                        {
                            while (queue.isFull())
                            {
                            }
                            Int64 tmp = 0;
                            ASSERT_EQ(queue.nonBlockingPush(std::move(tmp)), MPMCQueueResult::OK);
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

TEST_F(ConcurrentIOQueueTest, ConcurrentNonBlockingPushAndTryPop)
try
{
    for (size_t producer = 1; producer < 25; producer += 4)
    {
        for (size_t consumer = 1; consumer < 25; consumer += 4)
        {
            for (size_t queue_size = 1; queue_size < 25; queue_size += 4)
            {
                auto thread_manager = newThreadManager();
                ConcurrentIOQueue<Int64> queue(queue_size);

                std::atomic_size_t producer_num = producer;
                size_t produce_count_per_producer = 1000;
                std::atomic_size_t total_count = producer_num * produce_count_per_producer;
                for (size_t i = 0; i < producer_num; ++i)
                {
                    thread_manager->schedule(false, "producer", [&]() {
                        for (size_t i = 0; i < produce_count_per_producer; ++i)
                        {
                            while (queue.isFull())
                            {
                            }
                            Int64 tmp = 0;
                            ASSERT_EQ(queue.nonBlockingPush(std::move(tmp)), MPMCQueueResult::OK);
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

} // namespace
} // namespace DB::tests

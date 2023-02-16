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
#include <TestUtils/TiFlashTestBasic.h>

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
    {
        size_t max_size = 10;
        ConcurrentIOQueue<Int64> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_TRUE(queue.push(i) == MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE(queue.nonBlockingPush(max_size) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE((max_size + 1) == queue.size());

        for (size_t i = 0; i < max_size; ++i)
            ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isFull());
        ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.tryPop(i) == MPMCQueueResult::EMPTY);
    }

    {
        size_t max_size = 10;
        ConcurrentIOQueue<Int64> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_TRUE(queue.push(i) == MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE(queue.nonBlockingPush(max_size) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE((max_size + 1) == queue.size());

        queue.finish();

        for (size_t i = 0; i < max_size; ++i)
            ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(!queue.isFull());
        ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::FINISHED);
        ASSERT_TRUE(queue.tryPop(i) == MPMCQueueResult::FINISHED);
    }

    {
        size_t max_size = 10;
        ConcurrentIOQueue<Int64> queue(max_size);
        for (size_t i = 0; i < max_size; ++i)
        {
            ASSERT_TRUE(!queue.isFull());
            ASSERT_TRUE(queue.push(i) == MPMCQueueResult::OK);
        }
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE(queue.nonBlockingPush(max_size) == MPMCQueueResult::OK);
        ASSERT_TRUE(queue.isFull());
        ASSERT_TRUE((max_size + 1) == queue.size());

        queue.cancel();
        ASSERT_TRUE(!queue.isFull());
        ASSERT_TRUE(queue.pop(i) == MPMCQueueResult::CANCELLED);
        ASSERT_TRUE(queue.tryPop(i) == MPMCQueueResult::CANCELLED);
    }
}
CATCH

} // namespace
} // namespace DB::tests

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

#include <Operators/SharedQueue.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class TestSharedQueue : public ::testing::Test
{
};

TEST_F(TestSharedQueue, base)
try
{
    auto shared_queue = SharedQueue::buildInternal(2, 1, 0);
    {
        Block block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})};
        ASSERT_EQ(shared_queue->tryPush(std::move(block)), MPMCQueueResult::OK);
        shared_queue->producerFinish();
    }
    {
        Block block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})};
        ASSERT_EQ(shared_queue->tryPush(std::move(block)), MPMCQueueResult::OK);
        shared_queue->producerFinish();
    }
    {
        Block block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})};
        ASSERT_EQ(shared_queue->tryPush(std::move(block)), MPMCQueueResult::FINISHED);
    }

    Block block;
    ASSERT_EQ(shared_queue->tryPop(block), MPMCQueueResult::OK);
    ASSERT_TRUE(block);
    ASSERT_EQ(shared_queue->tryPop(block), MPMCQueueResult::OK);
    ASSERT_TRUE(block);
    ASSERT_EQ(shared_queue->tryPop(block), MPMCQueueResult::FINISHED);
}
CATCH

} // namespace DB::tests

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

#include <Common/Exception.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <tuple>

namespace DB
{
namespace tests
{
class TestGRPCReceiveQueue : public testing::Test
{
protected:
    TestGRPCReceiveQueue()
        : queue(2, [this](KickReceiveTag * t) -> grpc_call_error {
            bool placeholder;
            void * tag;
            t->FinalizeResult(&tag, &placeholder);
            tags.insert(tag);
            return grpc_call_error::GRPC_CALL_OK;
        })
    {}

    std::set<void *> tags;
    GRPCReceiveQueue<int> queue;

public:
    void checkTag(void * t)
    {
        if (t == nullptr && tags.empty())
            return;

        auto iter = tags.find(t);
        GTEST_ASSERT_NE(iter, tags.end());
        tags.erase(iter);
    }

    void checkTagInQueue(void * t)
    {
        if (t == nullptr && queue.tags.empty())
            return;

        bool find = false;
        for (auto * item : queue.tags)
            if (item == t)
                find = true;
        GTEST_ASSERT_EQ(find, true);
    }
};

TEST_F(TestGRPCReceiveQueue, SequentialWithOnePushSource)
try
{
    int p1;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    // Pop data, here we need to kick CompletionQueue
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    // Then we can push again
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Full again
    GTEST_ASSERT_EQ(queue.push(4, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    // Pop could be success
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(&p1);
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 3);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Finish it
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCReceiveQueue, SequentialWithMultiPushSources)
try
{
    int p1, p2, p3;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p2), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p3), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTag(nullptr);

    // Pop data, here we need to kick CompletionQueue
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTag(&p1);

    // Then we can push again
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTag(nullptr);

    // Full again
    GTEST_ASSERT_EQ(queue.push(4, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    // Pop could be success
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(&p3);
    checkTagInQueue(&p1);
    checkTag(&p2);
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 3);
    checkTagInQueue(&p1);
    checkTag(&p3);

    // Finish it
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(&p1); // p1 pushes data and the queue is full, so it was put into the GRPCReceiveQueue before

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCReceiveQueue, SequentialPopAfterFinish)
try
{
    int p1;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    // Finish and `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    // Pop could be success
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Next finish should fail.
    GTEST_ASSERT_EQ(queue.finish(), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCReceiveQueue, SequentialPopAfterCancel)
try
{
    int p1;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    // Cancel the queue
    GTEST_ASSERT_EQ(queue.cancelWith("cancel test"), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.getCancelReason(), "cancel test");
}
CATCH

TEST_F(TestGRPCReceiveQueue, SequentialFinishWithMultiPushSources)
try
{
    int p1, p2, p3;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p2), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p3), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTag(nullptr);

    // Finish it
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);
    checkTag(&p2);
    checkTag(&p3);

    // Pop could be success
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.pop(data), true);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCReceiveQueue, SequentialCanelWithMultiPushSources)
try
{
    int p1, p2, p3;
    int data;

    GTEST_ASSERT_EQ(queue.push(1, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2, &p1), GRPCReceiveQueueRes::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p1), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p2), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(3, &p3), GRPCReceiveQueueRes::FULL);
    checkTagInQueue(&p1);
    checkTagInQueue(&p2);
    checkTagInQueue(&p3);
    checkTag(nullptr);

    // Cancel the queue
    GTEST_ASSERT_EQ(queue.cancelWith("cancel test"), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);
    checkTag(&p2);
    checkTag(&p3);

    GTEST_ASSERT_EQ(queue.pop(data), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.getCancelReason(), "cancel test");
}
CATCH

} // namespace tests
} // namespace DB

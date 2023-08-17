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

#include <Common/Exception.h>
#include <Common/GRPCQueue.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <tuple>

namespace DB
{
namespace tests
{

class TestGRPCSendQueue : public testing::Test
{
protected:
    TestGRPCSendQueue()
        : tag(nullptr)
        , queue(Logger::get(), 10)
    {
        queue.setKickFuncForTest([this](GRPCKickTag * t) -> grpc_call_error {
            tag = t;
            return grpc_call_error::GRPC_CALL_OK;
        });
    }

    GRPCKickTag * tag;
    GRPCSendQueue<int> queue;

public:
    void checkTag(GRPCKickTag * t)
    {
        GTEST_ASSERT_EQ(t, tag);
        tag = nullptr;
    }

    void checkTagInQueue(GRPCKickTag * t) { GTEST_ASSERT_EQ(t, queue.tag); }
};

TEST_F(TestGRPCSendQueue, Sequential)
try
{
    DummyGRPCKickTag p1, p2, p3;
    int data;
    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.popWithTag(data, &p2), MPMCQueueResult::EMPTY);
    checkTagInQueue(&p2);
    checkTag(nullptr);

    // `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.push(3), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(&p2);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p3), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 3);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.popWithTag(data, &p3), MPMCQueueResult::EMPTY);
    checkTagInQueue(&p3);
    checkTag(nullptr);

    // `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(&p3);

    // Next finish should fail.
    GTEST_ASSERT_EQ(queue.finish(), false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.popWithTag(data, &p3), MPMCQueueResult::FINISHED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCSendQueue, SequentialPopAfterFinish)
try
{
    DummyGRPCKickTag p1;
    int data;
    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::EMPTY);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    GTEST_ASSERT_EQ(queue.push(2), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Finish the `queue` while some messages still exist in `queue`.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::FINISHED);
    GTEST_ASSERT_EQ(queue.forcePush(1), MPMCQueueResult::FINISHED);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::FINISHED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCSendQueue, SequentialPopAfterCancel)
try
{
    DummyGRPCKickTag p1;
    int data;

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.push(2), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.push(3), MPMCQueueResult::OK);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Cancel the queue.
    GTEST_ASSERT_EQ(queue.cancelWith("cancel test"), true);

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::CANCELLED);
    GTEST_ASSERT_EQ(queue.forcePush(1), MPMCQueueResult::CANCELLED);

    GTEST_ASSERT_EQ(queue.popWithTag(data, &p1), MPMCQueueResult::CANCELLED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.getCancelReason(), "cancel test");
}
CATCH

class TestGRPCRecvQueue : public testing::Test
{
protected:
    TestGRPCRecvQueue()
        : queue(Logger::get(), 2)
    {
        queue.setKickFuncForTest([this](GRPCKickTag * t) -> grpc_call_error {
            tags.emplace_back(t);
            return grpc_call_error::GRPC_CALL_OK;
        });
    }

    std::vector<GRPCKickTag *> tags;
    GRPCRecvQueue<int> queue;

public:
    void checkTags(std::vector<GRPCKickTag *> && expected_tags = {}, std::optional<bool> check_status = {})
    {
        GTEST_ASSERT_EQ(expected_tags, tags);
        if (check_status.has_value())
        {
            for (auto & t : tags)
                GTEST_ASSERT_EQ(t->getStatus(), check_status.value());
        }
        tags.clear();
    }

    void checkDataTagsInQueue(std::deque<std::pair<int, GRPCKickTag *>> && expected_data_tags = {})
    {
        GTEST_ASSERT_EQ(expected_data_tags, queue.data_tags);
    }
};

TEST_F(TestGRPCRecvQueue, Sequential)
try
{
    DummyGRPCKickTag p1, p2, p3;
    int data;
    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.pushWithTag(2, &p1), MPMCQueueResult::OK);

    GTEST_ASSERT_EQ(queue.pop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);

    GTEST_ASSERT_EQ(queue.pop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 2);

    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::EMPTY);
    checkDataTagsInQueue();
    checkTags();

    GTEST_ASSERT_EQ(queue.pushWithTag(1, &p1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.push(2), MPMCQueueResult::OK);
    // `queue` is full, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pushWithTag(3, &p2), MPMCQueueResult::FULL);
    checkDataTagsInQueue({{3, &p2}});
    checkTags();

    GTEST_ASSERT_EQ(queue.pushWithTag(4, &p3), MPMCQueueResult::FULL);
    checkDataTagsInQueue({{3, &p2}, {4, &p3}});
    checkTags();

    GTEST_ASSERT_EQ(queue.forcePush(5), MPMCQueueResult::OK);
    checkDataTagsInQueue({{3, &p2}, {4, &p3}});
    checkTags();

    // The queue is still full.
    GTEST_ASSERT_EQ(queue.pop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkDataTagsInQueue({{3, &p2}, {4, &p3}});
    checkTags();

    // `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkDataTagsInQueue({{4, &p3}});
    checkTags({&p2});

    // 5 is pushed by force.
    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 5);
    checkDataTagsInQueue();
    checkTags({&p3});

    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 3);
    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 4);
    checkDataTagsInQueue();
    checkTags();
}
CATCH

TEST_F(TestGRPCRecvQueue, SequentialPopAfterFinish)
try
{
    DummyGRPCKickTag p1, p2, p3;
    int data;
    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.pushWithTag(2, &p1), MPMCQueueResult::OK);
    // `queue` is full, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pushWithTag(3, &p2), MPMCQueueResult::FULL);
    GTEST_ASSERT_EQ(queue.pushWithTag(4, &p3), MPMCQueueResult::FULL);
    checkDataTagsInQueue({{3, &p2}, {4, &p3}});
    checkTags();

    // Finish the `queue` while some messages still exist in `queue`.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkDataTagsInQueue();
    checkTags({&p2, &p3}, {false});

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::FINISHED);
    GTEST_ASSERT_EQ(queue.forcePush(1), MPMCQueueResult::FINISHED);
    GTEST_ASSERT_EQ(queue.pushWithTag(1, &p1), MPMCQueueResult::FINISHED);

    GTEST_ASSERT_EQ(queue.pop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 1);
    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkDataTagsInQueue();
    checkTags();

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.tryPop(data), MPMCQueueResult::FINISHED);
}
CATCH

TEST_F(TestGRPCRecvQueue, SequentialPopAfterCancel)
try
{
    DummyGRPCKickTag p1, p2, p3;
    int data;
    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::OK);
    GTEST_ASSERT_EQ(queue.pushWithTag(2, &p1), MPMCQueueResult::OK);
    // `queue` is full, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pushWithTag(3, &p2), MPMCQueueResult::FULL);
    GTEST_ASSERT_EQ(queue.pushWithTag(4, &p3), MPMCQueueResult::FULL);
    checkDataTagsInQueue({{3, &p2}, {4, &p3}});
    checkTags();

    // Cancel the queue.
    GTEST_ASSERT_EQ(queue.cancelWith("cancel test"), true);
    checkDataTagsInQueue();
    checkTags({&p2, &p3}, {false});

    GTEST_ASSERT_EQ(queue.push(1), MPMCQueueResult::CANCELLED);
    GTEST_ASSERT_EQ(queue.forcePush(1), MPMCQueueResult::CANCELLED);
    GTEST_ASSERT_EQ(queue.pushWithTag(1, &p1), MPMCQueueResult::CANCELLED);

    GTEST_ASSERT_EQ(queue.pop(data), MPMCQueueResult::CANCELLED);
    checkDataTagsInQueue();
    checkTags();

    GTEST_ASSERT_EQ(queue.getCancelReason(), "cancel test");
}
CATCH

} // namespace tests
} // namespace DB

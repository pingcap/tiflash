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
#include <Flash/Mpp/GRPCSendQueue.h>
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
        , queue(10, [this](KickTag * t) -> grpc_call_error {
            bool no_use;
            t->FinalizeResult(&tag, &no_use);
            return grpc_call_error::GRPC_CALL_OK;
        })
    {}
    void * tag;
    GRPCSendQueue<int> queue;

public:
    void checkTag(void * t)
    {
        GTEST_ASSERT_EQ(t, tag);
        tag = nullptr;
    }

    void checkTagInQueue(void * t)
    {
        GTEST_ASSERT_EQ(t, queue.tag);
    }
};

TEST_F(TestGRPCSendQueue, Sequential)
try
{
    int p1, p2, p3;
    int data;
    GTEST_ASSERT_EQ(queue.push(1), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, &p2), GRPCSendQueueRes::EMPTY);
    checkTagInQueue(&p2);
    checkTag(nullptr);

    // `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.push(3), true);
    checkTagInQueue(nullptr);
    checkTag(&p2);

    GTEST_ASSERT_EQ(queue.pop(data, &p3), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 3);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, &p3), GRPCSendQueueRes::EMPTY);
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
    GTEST_ASSERT_EQ(queue.pop(data, &p3), GRPCSendQueueRes::FINISHED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCSendQueue, SequentialPopAfterFinish)
try
{
    int p1;
    int data;
    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::EMPTY);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(1), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    GTEST_ASSERT_EQ(queue.push(2), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Finish the `queue` while some messages still exist in `queue`.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished and empty.
    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::FINISHED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCSendQueue, SequentialPopAfterCancel)
try
{
    int p1;
    int data;

    GTEST_ASSERT_EQ(queue.push(1), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(2), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(3), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::OK);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Cancel the queue
    GTEST_ASSERT_EQ(queue.cancelWith("cancel test"), true);

    GTEST_ASSERT_EQ(queue.pop(data, &p1), GRPCSendQueueRes::CANCELLED);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.getCancelReason(), "cancel test");
}
CATCH

} // namespace tests
} // namespace DB
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
        : kick_tag(nullptr)
        , queue(10, [this](void * t) -> grpc_call_error {
            kick_tag = t;
            return grpc_call_error::GRPC_CALL_OK;
        })
    {}
    void * kick_tag;
    GRPCSendQueue<int> queue;

public:
    void checkTag(void * tag)
    {
        GTEST_ASSERT_EQ(tag, kick_tag);
        kick_tag = nullptr;
    }

    void checkTagInQueue(void * tag)
    {
        GTEST_ASSERT_EQ(tag, queue.tag);
    }
};

TEST_F(TestGRPCSendQueue, Sequential)
try
{
    int p1, p2, p3;
    int data;
    bool ok;
    GTEST_ASSERT_EQ(queue.push(1), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
    GTEST_ASSERT_EQ(queue.push(2), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), true);
    GTEST_ASSERT_EQ(ok, true);
    GTEST_ASSERT_EQ(data, 1);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), true);
    GTEST_ASSERT_EQ(ok, true);
    GTEST_ASSERT_EQ(data, 2);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, ok, &p2), false);
    checkTagInQueue(&p2);
    checkTag(nullptr);

    // `tag` should be gotten.
    GTEST_ASSERT_EQ(queue.push(3), true);
    checkTagInQueue(nullptr);
    checkTag(&p2);

    GTEST_ASSERT_EQ(queue.pop(data, ok, &p3), true);
    GTEST_ASSERT_EQ(ok, true);
    GTEST_ASSERT_EQ(data, 3);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, ok, &p3), false);
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

    // `queue` is finished so `ok` is false.
    GTEST_ASSERT_EQ(queue.pop(data, ok, &p3), true);
    GTEST_ASSERT_EQ(ok, false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

TEST_F(TestGRPCSendQueue, SequentialPopAfterFinish)
try
{
    int p1;
    int data;
    bool ok;
    // `queue` is empty, `tag` should be saved.
    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), false);
    checkTagInQueue(&p1);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.push(1), true);
    checkTagInQueue(nullptr);
    checkTag(&p1);

    GTEST_ASSERT_EQ(queue.push(2), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), true);
    GTEST_ASSERT_EQ(data, 1);
    GTEST_ASSERT_EQ(ok, true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // Finish the `queue` while some messages still exist in `queue`.
    GTEST_ASSERT_EQ(queue.finish(), true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), true);
    GTEST_ASSERT_EQ(data, 2);
    GTEST_ASSERT_EQ(ok, true);
    checkTagInQueue(nullptr);
    checkTag(nullptr);

    // `queue` is finished so `ok` is false.
    GTEST_ASSERT_EQ(queue.pop(data, ok, &p1), true);
    GTEST_ASSERT_EQ(ok, false);
    checkTagInQueue(nullptr);
    checkTag(nullptr);
}
CATCH

} // namespace tests
} // namespace DB
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
#include <Common/Logger.h>
#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/MemoryTracker.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>


namespace DB
{
namespace tests
{
namespace
{
TrackedMppDataPacketPtr newDataPacket(const String & data)
{
    auto data_packet_ptr = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    data_packet_ptr->getPacket().set_data(data);
    return data_packet_ptr;
}
} // namespace

class TestReceivedMessageQueue : public testing::Test
{
protected:
    void SetUp() override
    {
        mock_async_request_handler_wait_queue = std::make_shared<AsyncRequestHandlerWaitQueue>();
        log = std::make_shared<Logger>("TestReceivedMessageQueue");
    }
    void TearDown() override {}

    AsyncRequestHandlerWaitQueuePtr mock_async_request_handler_wait_queue;
    LoggerPtr log;

public:
};

TEST_F(TestReceivedMessageQueue, FineGrainedStreamSize)
try
{
    std::vector<bool> enable_fine_grained{false, true};
    std::vector<size_t> fine_grained_stream_count{1, 10};
    for (bool fine_grained : enable_fine_grained)
    {
        for (size_t fine_grained_size : fine_grained_stream_count)
        {
            ReceivedMessageQueue queue(mock_async_request_handler_wait_queue, log, 10, fine_grained, fine_grained_size);
            if (fine_grained)
                /// 1. fine grained size should > 0 if enable fine grained
                ASSERT_TRUE(queue.getFineGrainedStreamSize() > 0);
            else
                /// 2. fine grained size should == 0 if not enable fine grained
                ASSERT_TRUE(queue.getFineGrainedStreamSize() == 0);
        }
    }
}
CATCH

TEST_F(TestReceivedMessageQueue, UseMessageChannel)
try
{
    std::vector<size_t> queue_buffer_size{1, 10};
    std::vector<bool> enable_fine_grained{false, true};
    std::vector<size_t> fine_grained_stream_count{1, 10};
    for (size_t buffer_size : queue_buffer_size)
    {
        for (bool fine_grained : enable_fine_grained)
        {
            for (size_t fine_grained_stream_size : fine_grained_stream_count)
            {
                ReceivedMessageQueue queue(mock_async_request_handler_wait_queue, log, buffer_size, fine_grained, fine_grained_stream_size);
                for (size_t i = 0; i < buffer_size; ++i)
                {
                    auto message = toReceivedMessage(newDataPacket(fmt::format("test_{}", i)), 0, "mock", fine_grained, fine_grained_stream_size);
                    /// is_force = false
                    auto result = queue.pushToMessageChannel<false>(message, ReceiverMode::Async);
                    ASSERT_TRUE(result);
                }
                ASSERT_TRUE(!queue.isWritable());
                auto message = toReceivedMessage(newDataPacket(fmt::format("test_{}", buffer_size)), 0, "mock", fine_grained, fine_grained_stream_size);
                /// is_force = true
                auto result = queue.pushToMessageChannel<true>(message, ReceiverMode::Async);
                ASSERT_TRUE(result);
                if (fine_grained)
                {
                    for (size_t i = 0; i <= buffer_size; ++i)
                    {
                        for (size_t k = 0; k < fine_grained_stream_size; k++)
                        {
                            ReceivedMessagePtr recv_msg;
                            auto pop_result = queue.pop<false>(k, recv_msg);
                            ASSERT_TRUE(pop_result == MPMCQueueResult::OK);
                            if (k == 0)
                                ASSERT_TRUE(*recv_msg->getRespPtr(k) == fmt::format("test_{}", i));
                            else
                                ASSERT_TRUE(recv_msg->getRespPtr(k) == nullptr);
                        }
                    }
                }
                else
                {
                    for (size_t i = 0; i <= buffer_size; ++i)
                    {
                        ReceivedMessagePtr recv_msg;
                        auto pop_result = queue.pop<false>(0, recv_msg);
                        ASSERT_TRUE(pop_result == MPMCQueueResult::OK);
                        ASSERT_TRUE(*recv_msg->getRespPtr(0) == fmt::format("test_{}", i));
                    }
                }
                ASSERT_TRUE(queue.isWritable());
            }
        }
    }
}
CATCH

TEST_F(TestReceivedMessageQueue, UseGRPCRecvQueue)
try
{
    std::vector<size_t> queue_buffer_size{1, 10};
    std::vector<bool> enable_fine_grained{false, true};
    std::vector<size_t> fine_grained_stream_count{1, 10};
    for (size_t buffer_size : queue_buffer_size)
    {
        for (bool fine_grained : enable_fine_grained)
        {
            for (size_t fine_grained_stream_size : fine_grained_stream_count)
            {
                ReceivedMessageQueue queue(mock_async_request_handler_wait_queue, log, buffer_size, fine_grained, fine_grained_stream_size);
                for (size_t i = 0; i < buffer_size; ++i)
                {
                    auto message = toReceivedMessage(newDataPacket(fmt::format("test_{}", i)), 0, "mock", fine_grained, fine_grained_stream_size);
                    auto result = queue.pushToGRPCReceiveQueue(message);
                    ASSERT_TRUE(result == GRPCReceiveQueueRes::OK);
                }
                ASSERT_TRUE(!queue.isWritable());
                auto message = toReceivedMessage(newDataPacket(fmt::format("test_{}", buffer_size)), 0, "mock", fine_grained, fine_grained_stream_size);
                auto result = queue.pushToGRPCReceiveQueue(message);
                ASSERT_TRUE(result == GRPCReceiveQueueRes::FULL);
                if (fine_grained)
                {
                    for (size_t i = 0; i < buffer_size; ++i)
                    {
                        for (size_t k = 0; k < fine_grained_stream_size; k++)
                        {
                            ReceivedMessagePtr recv_msg;
                            auto pop_result = queue.pop<false>(k, recv_msg);
                            ASSERT_TRUE(pop_result == MPMCQueueResult::OK);
                            if (k == 0)
                                ASSERT_TRUE(*recv_msg->getRespPtr(k) == fmt::format("test_{}", i));
                            else
                                ASSERT_TRUE(recv_msg->getRespPtr(k) == nullptr);
                        }
                    }
                }
                else
                {
                    for (size_t i = 0; i < buffer_size; ++i)
                    {
                        ReceivedMessagePtr recv_msg;
                        auto pop_result = queue.pop<false>(0, recv_msg);
                        ASSERT_TRUE(pop_result == MPMCQueueResult::OK);
                        ASSERT_TRUE(*recv_msg->getRespPtr(0) == fmt::format("test_{}", i));
                    }
                }
                ASSERT_TRUE(queue.isWritable());
            }
        }
    }
}
CATCH

} // namespace tests
} // namespace DB

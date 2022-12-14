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
#include <Flash/EstablishCall.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace DB
{
namespace tests
{
namespace
{
TrackedMppDataPacketPtr newDataPacket(const String & data)
{
    auto data_packet_ptr = std::make_shared<TrackedMppDataPacket>();
    data_packet_ptr->getPacket().set_data(data);
    return data_packet_ptr;
}
} // namespace

class MockPacketWriter : public PacketWriter
{
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data().empty() ? packet.error().msg() : packet.data());
        return true;
    }

public:
    std::vector<String> write_packet_vec;
};

class MockFailedWriter : public PacketWriter
{
    bool write(const mpp::MPPDataPacket &) override
    {
        return false;
    }
};

class MockAsyncCallData : public IAsyncCallData
{
public:
    MockAsyncCallData() = default;

    void attachAsyncTunnelSender(const std::shared_ptr<AsyncTunnelSender> & async_tunnel_sender_) override
    {
        async_tunnel_sender = async_tunnel_sender_;
    }

    grpc_call * grpcCall() override
    {
        return nullptr;
    }

    std::optional<GRPCKickFunc> getKickFuncForTest() override
    {
        return [&](KickTag * tag) {
            {
                void * t;
                bool s;
                tag->FinalizeResult(&t, &s);
                std::unique_lock<std::mutex> lock(mu);
                has_msg = true;
            }
            cv.notify_one();
            return grpc_call_error::GRPC_CALL_OK;
        };
    }

    void run()
    {
        while (true)
        {
            TrackedMppDataPacketPtr res;
            switch (async_tunnel_sender->pop(res, this))
            {
            case GRPCSendQueueRes::OK:
                if (write_failed)
                {
                    async_tunnel_sender->consumerFinish(fmt::format("{} meet error: grpc writes failed.", async_tunnel_sender->getTunnelId()));
                    return;
                }
                write_packet_vec.push_back(res->packet.data());
                break;
            case GRPCSendQueueRes::FINISHED:
                async_tunnel_sender->consumerFinish("");
                return;
            case GRPCSendQueueRes::CANCELLED:
                assert(!async_tunnel_sender->getCancelReason().empty());
                if (write_failed)
                {
                    async_tunnel_sender->consumerFinish(fmt::format("{} meet error: {}.", async_tunnel_sender->getTunnelId(), async_tunnel_sender->getCancelReason()));
                    return;
                }
                write_packet_vec.push_back(async_tunnel_sender->getCancelReason());
                async_tunnel_sender->consumerFinish("");
                return;
            case GRPCSendQueueRes::EMPTY:
                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock, [&] {
                    return has_msg;
                });
                has_msg = false;
                break;
            }
        }
    }

    std::shared_ptr<DB::AsyncTunnelSender> async_tunnel_sender;
    std::vector<String> write_packet_vec;

    std::mutex mu;
    std::condition_variable cv;
    bool has_msg = false;
    bool write_failed = false;
};

class TestMPPTunnel : public testing::Test
{
protected:
    void SetUp() override { timeout = std::chrono::seconds(10); }
    void TearDown() override {}
    std::chrono::seconds timeout{};

public:
    MPPTunnelPtr constructRemoteSyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, false, false, String("0"));
        return tunnel;
    }

    MPPTunnelPtr constructRemoteAsyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, false, true, String("0"));
        return tunnel;
    }

    static void waitSyncTunnelSenderThread(SyncTunnelSenderPtr sync_tunnel_sender)
    {
        sync_tunnel_sender->thread_manager->wait();
    }

    static void setTunnelFinished(MPPTunnelPtr tunnel)
    {
        tunnel->status = MPPTunnel::TunnelStatus::Finished;
    }

    static bool getTunnelConnectedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status != MPPTunnel::TunnelStatus::Unconnected && tunnel->status != MPPTunnel::TunnelStatus::Finished;
    }

    static bool getTunnelFinishedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status == MPPTunnel::TunnelStatus::Finished;
    }

    static bool getTunnelSenderConsumerFinishedFlag(TunnelSenderPtr sender)
    {
        return sender->isConsumerFinished();
    }
};

TEST_F(TestMPPTunnel, ConnectWhenFinished)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    setTunnelFinished(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connect(nullptr);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected or finished: Finished");
}

TEST_F(TestMPPTunnel, ConnectWhenConnected)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected or finished: Connected");
    }
}

TEST_F(TestMPPTunnel, CloseBeforeConnect)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), false);
}
CATCH

TEST_F(TestMPPTunnel, CloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
}
CATCH

TEST_F(TestMPPTunnel, WriteAfterUnconnectFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        setTunnelFinished(mpp_tunnel_ptr);
        mpp_tunnel_ptr->write(newDataPacket("First"));
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed.");
    }
}

TEST_F(TestMPPTunnel, WriteDoneAfterUnconnectFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        setTunnelFinished(mpp_tunnel_ptr);
        mpp_tunnel_ptr->writeDone();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed.");
    }
}

TEST_F(TestMPPTunnel, ConnectWriteCancel)
try
{
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->close("Cancel", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    auto result_size = dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec.size();
    // close will cancel the MPMCQueue, so there is no guarantee that all the message will be consumed, only the last error packet
    // must to be consumed
    GTEST_ASSERT_EQ(result_size >= 1 && result_size <= 2, true);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec[result_size - 1], "Cancel");
}
CATCH

TEST_F(TestMPPTunnel, ConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, ConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->getSyncTunnelSender()->consumerFinish("");
    waitSyncTunnelSenderThread(mpp_tunnel_ptr->getSyncTunnelSender());

    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, WriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockFailedWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->write(newDataPacket("First"));
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, 0000_0001 meet error: grpc writes failed.");
    }
}

TEST_F(TestMPPTunnel, WriteAfterFinished)
{
    std::unique_ptr<PacketWriter> writer_ptr = nullptr;
    MPPTunnelPtr mpp_tunnel_ptr = nullptr;
    try
    {
        mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = std::make_unique<MockPacketWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->close("Canceled", false);
        mpp_tunnel_ptr->write(newDataPacket("First"));
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
    }
    if (mpp_tunnel_ptr != nullptr)
        mpp_tunnel_ptr->waitForFinish();
}

/// Test Async MPPTunnel
TEST_F(TestMPPTunnel, AsyncConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<MockAsyncCallData> call_data = std::make_unique<MockAsyncCallData>();
    mpp_tunnel_ptr->connectAsync(call_data.get());

    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::thread t(&MockAsyncCallData::run, call_data.get());

    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->write(newDataPacket("Second"));
    mpp_tunnel_ptr->close("Cancel", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);

    t.join();
    auto result_size = call_data->write_packet_vec.size();
    GTEST_ASSERT_EQ(result_size >= 1 && result_size <= 3, true); //Third for err msg
    GTEST_ASSERT_EQ(call_data->write_packet_vec[result_size - 1], "Cancel");
}
CATCH

TEST_F(TestMPPTunnel, AsyncConnectWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<MockAsyncCallData> call_data = std::make_unique<MockAsyncCallData>();
    mpp_tunnel_ptr->connectAsync(call_data.get());

    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::thread t(&MockAsyncCallData::run, call_data.get());

    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();

    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);

    t.join();
    GTEST_ASSERT_EQ(call_data->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(call_data->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, AsyncConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<MockAsyncCallData> call_data = std::make_unique<MockAsyncCallData>();
    mpp_tunnel_ptr->connectAsync(call_data.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::thread t(&MockAsyncCallData::run, call_data.get());

    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->getTunnelSender()->consumerFinish("");
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);

    t.join();
    GTEST_ASSERT_EQ(call_data->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(call_data->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, AsyncWriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
        std::unique_ptr<MockAsyncCallData> call_data = std::make_unique<MockAsyncCallData>();
        call_data->write_failed = true;
        mpp_tunnel_ptr->connectAsync(call_data.get());

        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

        std::thread t(&MockAsyncCallData::run, call_data.get());

        mpp_tunnel_ptr->write(newDataPacket("First"));
        t.join();
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, 0000_0001 meet error: grpc writes failed.");
    }
}

} // namespace tests
} // namespace DB
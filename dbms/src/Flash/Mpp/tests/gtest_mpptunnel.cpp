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
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Flash/EstablishCall.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
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

class MockExchangeReceiver
{
public:
    explicit MockExchangeReceiver(Int32 conn_num)
        : live_connections(conn_num)
        , live_local_connections(0)
        , data_size_in_queue(0)
        , log(Logger::get())
    {
        msg_channels.push_back(std::make_shared<ConcurrentIOQueue<std::shared_ptr<ReceivedMessage>>>(10));
    }

    void connectionDone(bool meet_error, const String & local_err_msg)
    {
        Int32 copy_connection = -1;
        {
            std::lock_guard<std::mutex> lock(mu);
            if (meet_error)
                err_msg = local_err_msg;
            --live_connections;
            copy_connection = live_connections;
        }

        if (meet_error || copy_connection == 0)
            msg_channels[0]->finish();
    }

    void addLocalConnectionNum()
    {
        std::lock_guard<std::mutex> lock(mu);
        ++live_local_connections;
    }

    void connectionLocalDone()
    {
        std::lock_guard<std::mutex> lock(mu);
        --live_local_connections;
    }

    void connectLocalTunnel(std::vector<MPPTunnelPtr> & tunnels)
    {
        if (static_cast<Int32>(tunnels.size()) != live_connections)
            throw Exception("conn_num != tunnels.size()");

        for (auto & tunnel : tunnels)
        {
            LocalRequestHandler local_request_handler(
                nullptr,
                [this](bool meet_error, const String & err_msg) {
                    this->connectionDone(meet_error, err_msg);
                },
                [this]() {
                    this->connectionLocalDone();
                },
                []() {},
                ReceiverChannelWriter(&msg_channels, "", log, &data_size_in_queue, ReceiverMode::Local));
            tunnel->connectLocalV2(0, local_request_handler, false, true);
        }
    }

    void receiveAll()
    {
        while (true)
        {
            std::shared_ptr<ReceivedMessage> recv_msg;
            switch (msg_channels[0]->pop(recv_msg))
            {
            case DB::MPMCQueueResult::OK:
                received_msgs.push_back(recv_msg);
                break;
            default:
                return;
            }
        };
    }

    std::vector<std::shared_ptr<ReceivedMessage>> & getReceivedMsgs() { return received_msgs; }

    String getErrMsg()
    {
        std::lock_guard<std::mutex> lock(mu);
        return err_msg;
    }

private:
    std::mutex mu;
    std::condition_variable cv;
    Int32 live_connections;
    Int32 live_local_connections;
    std::vector<MsgChannelPtr> msg_channels;
    String err_msg;
    std::vector<std::shared_ptr<ReceivedMessage>> received_msgs;
    std::atomic<Int64> data_size_in_queue;
    LoggerPtr log;
};

using MockExchangeReceiverPtr = std::shared_ptr<MockExchangeReceiver>;

template <typename Func>
void tunnelRun(Func && func)
{
    func();
}

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

    MPPTunnelPtr constructLocalTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, true, false, String("0"));
        return tunnel;
    }

    static void waitSyncTunnelSenderThread(SyncTunnelSenderPtr sync_tunnel_sender)
    {
        sync_tunnel_sender->thread_manager->wait();
    }

    static void setTunnelFinished(MPPTunnelPtr tunnel)
    {
        tunnel->status = MPPTunnel::TunnelStatus::Finished;
        if (tunnel->local_tunnel_v2)
            tunnel->local_tunnel_v2->is_done.store(true);
        else if (tunnel->local_tunnel_fine_grained_v2)
            tunnel->local_tunnel_fine_grained_v2->is_done.store(true);
        else if (tunnel->local_tunnel_local_only_v2)
            tunnel->local_tunnel_local_only_v2->is_done.store(true);
        else if (tunnel->local_tunnel_fine_grained_local_only_v2)
            tunnel->local_tunnel_fine_grained_local_only_v2->is_done.store(true);
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

    std::pair<MockExchangeReceiverPtr, std::vector<MPPTunnelPtr>> prepareLocal(const size_t tunnel_num)
    {
        MockExchangeReceiverPtr receiver = std::make_shared<MockExchangeReceiver>(tunnel_num);
        std::vector<MPPTunnelPtr> tunnels;
        for (size_t i = 0; i < tunnel_num; ++i)
            tunnels.push_back(constructLocalTunnel());

        receiver->connectLocalTunnel(tunnels);
        return std::pair<MockExchangeReceiverPtr, std::vector<MPPTunnelPtr>>(receiver, tunnels);
    }
};

/// Test Sync MPPTunnel
TEST_F(TestMPPTunnel, SyncConnectWhenFinished)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    setTunnelFinished(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connectSync(nullptr);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check status == TunnelStatus::Unconnected failed: MPPTunnel has connected or finished: Finished");
}

TEST_F(TestMPPTunnel, SyncConnectWhenConnected)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
        mpp_tunnel_ptr->connectSync(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->connectSync(writer_ptr.get());
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Check status == TunnelStatus::Unconnected failed: MPPTunnel has connected or finished: Connected");
    }
}

TEST_F(TestMPPTunnel, SyncCloseBeforeConnect)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), false);
}
CATCH

TEST_F(TestMPPTunnel, SyncCloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->close("Canceled", false);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
}
CATCH

TEST_F(TestMPPTunnel, SyncWriteAfterUnconnectFinished)
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
        GTEST_ASSERT_EQ(e.message(), "Check tunnel_sender != nullptr failed: write to tunnel 0000_0001 which is already closed.");
    }
}

TEST_F(TestMPPTunnel, SyncWriteDoneAfterUnconnectFinished)
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
        GTEST_ASSERT_EQ(e.message(), "write to tunnel 0000_0001 which is already closed.");
    }
}

TEST_F(TestMPPTunnel, SyncConnectWriteCancel)
try
{
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->connectSync(writer_ptr.get());
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

TEST_F(TestMPPTunnel, SyncConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    mpp_tunnel_ptr->connectSync(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, SyncConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockPacketWriter>();
    mpp_tunnel_ptr->connectSync(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->write(newDataPacket("First"));
    mpp_tunnel_ptr->getSyncTunnelSender()->consumerFinish("");
    waitSyncTunnelSenderThread(mpp_tunnel_ptr->getSyncTunnelSender());

    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockPacketWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, SyncWriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockFailedWriter>();
        mpp_tunnel_ptr->connectSync(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->write(newDataPacket("First"));
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "0000_0001: consumer exits unexpected, error message: 0000_0001 meet error: grpc writes failed. ");
    }
}

// TODO remove try-catch and get where throws the exception
TEST_F(TestMPPTunnel, SyncWriteAfterFinished)
{
    std::unique_ptr<PacketWriter> writer_ptr = nullptr;
    MPPTunnelPtr mpp_tunnel_ptr = nullptr;
    try
    {
        mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = std::make_unique<MockPacketWriter>();
        mpp_tunnel_ptr->connectSync(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->close("Canceled", false);
        mpp_tunnel_ptr->write(newDataPacket("First"));
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel 0000_0001 which is already closed, ");
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
        GTEST_ASSERT_EQ(e.message(), "0000_0001: consumer exits unexpected, error message: 0000_0001 meet error: grpc writes failed. ");
    }
}

/// Test Local MPPTunnel
TEST_F(TestMPPTunnel, LocalConnectWriteDone)
try
{
    const size_t tunnel_num = 3;
    size_t send_data_packet_num = 3;
    auto [receiver, tunnels] = prepareLocal(tunnel_num);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < tunnels.size(); ++i)
    {
        auto run_tunnel = [sender_tunnel = tunnels[i]->getLocalTunnelSenderV2(), send_data_packet_num]() {
            ASSERT_TRUE(sender_tunnel.get() != nullptr);
            for (size_t i = 0; i < send_data_packet_num; ++i)
                sender_tunnel->push(newDataPacket("111"));
            sender_tunnel->finish();
        };
        threads.push_back(std::thread(tunnelRun<decltype(run_tunnel)>, std::move(run_tunnel)));
    }

    receiver->receiveAll();

    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(receiver->getReceivedMsgs().size(), tunnel_num * send_data_packet_num);
}
CATCH

TEST_F(TestMPPTunnel, LocalConnectWriteCancel)
try
{
    auto [receiver, tunnels] = prepareLocal(1);

    std::thread t(&MockExchangeReceiver::receiveAll, receiver.get());

    tunnels[0]->write(newDataPacket("First"));
    tunnels[0]->write(newDataPacket("Second"));
    tunnels[0]->close("Cancel", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(tunnels[0]), true);

    t.join();
    auto result_size = receiver->getReceivedMsgs().size();
    GTEST_ASSERT_EQ(result_size >= 0 && result_size <= 2, true);
    GTEST_ASSERT_EQ(receiver->getErrMsg(), "Cancel");
}
CATCH

TEST_F(TestMPPTunnel, LocalConsumerFinish)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    std::thread t(&MockExchangeReceiver::receiveAll, receiver.get());

    tunnels[0]->write(newDataPacket("First"));
    tunnels[0]->write(newDataPacket("Second"));
    tunnels[0]->getLocalTunnelSenderV2()->consumerFinish("");
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(tunnels[0]->getLocalTunnelSenderV2()), true);

    t.join();
    auto result_size = receiver->getReceivedMsgs().size();
    GTEST_ASSERT_EQ(result_size == 2, true);
    GTEST_ASSERT_EQ(receiver->getReceivedMsgs()[0]->packet->getPacket().data(), "First");
    GTEST_ASSERT_EQ(receiver->getReceivedMsgs()[1]->packet->getPacket().data(), "Second");
}
CATCH

TEST_F(TestMPPTunnel, LocalConnectWhenFinished)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    setTunnelFinished(tunnels[0]);

    LocalRequestHandler local_req_handler(
        nullptr,
        [](bool, const String &) {},
        []() {},
        []() {},
        ReceiverChannelWriter(nullptr, "", Logger::get(), nullptr, ReceiverMode::Local));
    tunnels[0]->connectLocalV2(0, local_req_handler, false, false);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check status == TunnelStatus::Unconnected failed: MPPTunnel 0000_0001 has connected or finished: Finished");
}

TEST_F(TestMPPTunnel, LocalConnectWhenConnected)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(tunnels[0]), true);
    LocalRequestHandler local_req_handler(
        nullptr,
        [](bool, const String &) {},
        []() {},
        []() {},
        ReceiverChannelWriter(nullptr, "", Logger::get(), nullptr, ReceiverMode::Local));
    tunnels[0]->connectLocalV2(0, local_req_handler, false, false);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check status == TunnelStatus::Unconnected failed: MPPTunnel 0000_0001 has connected or finished: Connected");
}

TEST_F(TestMPPTunnel, LocalCloseBeforeConnect)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    tunnels[0]->close("Canceled", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(tunnels[0]), true);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(tunnels[0]), false);
}
CATCH

TEST_F(TestMPPTunnel, LocalCloseAfterClose)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    tunnels[0]->close("Canceled", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(tunnels[0]), true);
    tunnels[0]->close("Canceled", true);
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(tunnels[0]), true);
}
CATCH

TEST_F(TestMPPTunnel, LocalWriteAfterUnconnectFinished)
try
{
    auto tunnel = constructLocalTunnel();
    setTunnelFinished(tunnel);
    tunnel->write(newDataPacket("First"));
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check tunnel_sender != nullptr failed: write to tunnel 0000_0001 which is already closed.");
}

TEST_F(TestMPPTunnel, LocalWriteDoneAfterUnconnectFinished)
try
{
    auto tunnel = constructLocalTunnel();
    setTunnelFinished(tunnel);
    tunnel->writeDone();
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "write to tunnel 0000_0001 which is already closed.");
}

TEST_F(TestMPPTunnel, LocalWriteError)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(tunnels[0]), true);
    auto packet = newDataPacket("First");
    packet->error_message = "err";

    auto run_tunnel = [tunnel = tunnels[0]]() {
        try
        {
            auto packet = newDataPacket("First");
            packet->error_message = "err";
            tunnel->write(std::move(packet));
        }
        catch (...)
        {
        }
    };
    std::thread thd(tunnelRun<decltype(run_tunnel)>, std::move(run_tunnel));
    thd.join();

    tunnels[0]->waitForFinish();
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "0000_0001: consumer exits unexpected, error message: err ");
}

TEST_F(TestMPPTunnel, LocalWriteAfterFinished)
{
    MockExchangeReceiverPtr receiver_ptr;
    MPPTunnelPtr tunnel = nullptr;
    try
    {
        auto [receiver, tunnels] = prepareLocal(1);
        receiver_ptr = receiver;
        tunnel = tunnels[0];
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(tunnel), true);
        tunnel->close("", false);
        tunnel->write(newDataPacket("First"));
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel 0000_0001 which is already closed, ");
    }
    if (tunnel != nullptr)
        tunnel->waitForFinish();
}

TEST_F(TestMPPTunnel, SyncTunnelNonBlockingWrite)
{
    auto writer_ptr = std::make_unique<MockPacketWriter>();
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->connectSync(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    ASSERT_TRUE(mpp_tunnel_ptr->isReadyForWrite());
    mpp_tunnel_ptr->nonBlockingWrite(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);

    GTEST_ASSERT_EQ(writer_ptr->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(writer_ptr->write_packet_vec.back(), "First");
}

TEST_F(TestMPPTunnel, AsyncTunnelNonBlockingWrite)
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<MockAsyncCallData> call_data = std::make_unique<MockAsyncCallData>();
    mpp_tunnel_ptr->connectAsync(call_data.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    std::thread t(&MockAsyncCallData::run, call_data.get());

    ASSERT_TRUE(mpp_tunnel_ptr->isReadyForWrite());
    mpp_tunnel_ptr->nonBlockingWrite(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    t.join();

    GTEST_ASSERT_EQ(call_data->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(call_data->write_packet_vec.back(), "First");
}

TEST_F(TestMPPTunnel, LocalTunnelNonBlockingWrite)
{
    auto [receiver, tunnels] = prepareLocal(1);
    const auto & mpp_tunnel_ptr = tunnels.back();
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    std::thread t(&MockExchangeReceiver::receiveAll, receiver.get());

    ASSERT_TRUE(mpp_tunnel_ptr->isReadyForWrite());
    mpp_tunnel_ptr->nonBlockingWrite(newDataPacket("First"));
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    t.join();

    GTEST_ASSERT_EQ(receiver->getReceivedMsgs().size(), 1);
    GTEST_ASSERT_EQ(receiver->getReceivedMsgs().back()->packet->getPacket().data(), "First");
}

TEST_F(TestMPPTunnel, isReadyForWriteTimeout)
try
{
    timeout = std::chrono::seconds(1);
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    Stopwatch stop_watch{CLOCK_MONOTONIC_COARSE};
    while (stop_watch.elapsedSeconds() < 3 * timeout.count())
    {
        ASSERT_FALSE(mpp_tunnel_ptr->isReadyForWrite());
    }
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "0000_0001 is timeout");
}
} // namespace tests
} // namespace DB

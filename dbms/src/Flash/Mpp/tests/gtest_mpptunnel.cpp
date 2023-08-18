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
class MPPTunnelTest : public MPPTunnelBase<PacketWriter>
{
public:
    using Base = MPPTunnelBase<PacketWriter>;
    using Base::Base;
    MPPTunnelTest(
        const String & tunnel_id_,
        std::chrono::seconds timeout_,
        int input_steams_num_,
        bool is_local_,
        bool is_async_,
        const String & req_id)
        : Base(tunnel_id_, timeout_, input_steams_num_, is_local_, is_async_, req_id)
    {}
    void setFinishFlag(bool flag)
    {
        finished = flag;
    }
    bool getFinishFlag()
    {
        return finished;
    }
    bool getConnectFlag()
    {
        return connected;
    }
    std::shared_ptr<ThreadManager> getThreadManager()
    {
        return thread_manager;
    }
    LoggerPtr getLog()
    {
        return log;
    }
};

using MPPTunnelTestPtr = std::shared_ptr<MPPTunnelTest>;

class MockWriter : public PacketWriter
{
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        return true;
    }

public:
    std::vector<String> write_packet_vec;
};

class MockFailedWriter : public PacketWriter
{
    bool write(const mpp::MPPDataPacket &) override { return false; }
};

<<<<<<< HEAD
struct MockLocalReader
{
    MPPTunnelTestPtr tunnel;
=======
class MockAsyncCallData
    : public IAsyncCallData
    , public GRPCKickTag
{
public:
    MockAsyncCallData() = default;

    void attachAsyncTunnelSender(const std::shared_ptr<AsyncTunnelSender> & async_tunnel_sender_) override
    {
        async_tunnel_sender = async_tunnel_sender_;
    }

    std::optional<GRPCKickFunc> getGRPCKickFuncForTest() override
    {
        return [&](GRPCKickTag *) {
            {
                std::unique_lock<std::mutex> lock(mu);
                has_msg = true;
            }
            cv.notify_one();
            return grpc_call_error::GRPC_CALL_OK;
        };
    }

    void execute(bool) override {}

    void run()
    {
        while (true)
        {
            TrackedMppDataPacketPtr packet;
            auto res = async_tunnel_sender->popWithTag(packet, this);
            switch (res)
            {
            case MPMCQueueResult::OK:
                if (write_failed)
                {
                    async_tunnel_sender->consumerFinish(
                        fmt::format("{} meet error: grpc writes failed.", async_tunnel_sender->getTunnelId()));
                    return;
                }
                write_packet_vec.push_back(packet->packet.data());
                break;
            case MPMCQueueResult::FINISHED:
                async_tunnel_sender->consumerFinish("");
                return;
            case MPMCQueueResult::CANCELLED:
                assert(!async_tunnel_sender->getCancelReason().empty());
                if (write_failed)
                {
                    async_tunnel_sender->consumerFinish(fmt::format(
                        "{} meet error: {}.",
                        async_tunnel_sender->getTunnelId(),
                        async_tunnel_sender->getCancelReason()));
                    return;
                }
                write_packet_vec.push_back(async_tunnel_sender->getCancelReason());
                async_tunnel_sender->consumerFinish("");
                return;
            case MPMCQueueResult::EMPTY:
            {
                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock, [&] { return has_msg; });
                has_msg = false;
                break;
            }
            default:
                __builtin_unreachable();
            }
        }
    }

    std::shared_ptr<DB::AsyncTunnelSender> async_tunnel_sender;
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    std::vector<String> write_packet_vec;

    explicit MockLocalReader(const MPPTunnelTestPtr & tunnel_)
        : tunnel(tunnel_)
    {}

<<<<<<< HEAD
    ~MockLocalReader()
    {
        if (tunnel)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            LOG_FMT_TRACE(tunnel->getLog(), "before mocklocalreader invoking consumerFinish!");
            tunnel->consumerFinish("Receiver closed");
            LOG_FMT_TRACE(tunnel->getLog(), "after mocklocalreader invoking consumerFinish!");
=======
class MockExchangeReceiver
{
public:
    explicit MockExchangeReceiver(Int32 conn_num)
        : live_connections(conn_num)
        , live_local_connections(0)
        , data_size_in_queue(0)
        , received_message_queue(10, Logger::get(), &data_size_in_queue, false, 0)
        , log(Logger::get())
    {}

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
            received_message_queue.finish();
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
                [this](bool meet_error, const String & local_err_msg) {
                    this->connectionDone(meet_error, local_err_msg);
                },
                [this]() { this->connectionLocalDone(); },
                []() {},
                "",
                &received_message_queue);
            tunnel->connectLocalV2(0, local_request_handler, true);
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
        }
    }

    void read()
    {
        while (true)
        {
            MPPDataPacketPtr tmp_packet = tunnel->readForLocal();
            bool success = tmp_packet != nullptr;
            if (success)
            {
                write_packet_vec.push_back(tmp_packet->data());
            }
            else
            {
                break;
            }
        }
    }
};
using MockLocalReaderPtr = std::shared_ptr<MockLocalReader>;

struct MockTerminateLocalReader
{
    MPPTunnelTestPtr tunnel;

    explicit MockTerminateLocalReader(const MPPTunnelTestPtr & tunnel_)
        : tunnel(tunnel_)
    {}

    ~MockTerminateLocalReader()
    {
        if (tunnel)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            tunnel->consumerFinish("Receiver closed");
        }
    }

    void read() const
    {
        MPPDataPacketPtr tmp_packet = tunnel->readForLocal();
        tunnel->consumerFinish("Receiver closed");
    }
};
using MockTerminateLocalReaderPtr = std::shared_ptr<MockTerminateLocalReader>;


class MockAsyncWriter : public PacketWriter
{
public:
    explicit MockAsyncWriter(MPPTunnelTestPtr tunnel_)
        : tunnel(tunnel_)
    {}
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        // Simulate the async process, write success then check if exist msg, then write again
        if (tunnel->isSendQueueNextPopNonBlocking())
        {
            tunnel->sendJob(false);
        }
        return true;
    }

    void tryFlushOne() override
    {
        if (ready && tunnel->isSendQueueNextPopNonBlocking())
        {
            tunnel->sendJob(false);
        }
        ready = true;
    }
    MPPTunnelTestPtr tunnel;
    std::vector<String> write_packet_vec;
    bool ready = false;
};

class MockFailedAsyncWriter : public PacketWriter
{
public:
    explicit MockFailedAsyncWriter(MPPTunnelTestPtr tunnel_)
        : tunnel(tunnel_)
    {}
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        // Simulate the async process, write success then check if exist msg, then write again
        if (tunnel->isSendQueueNextPopNonBlocking())
        {
            tunnel->sendJob(false);
        }
        return false;
    }

    void tryFlushOne() override
    {
        if (ready && tunnel->isSendQueueNextPopNonBlocking())
        {
            tunnel->sendJob(false);
        }
        ready = true;
    }
    MPPTunnelTestPtr tunnel;
    std::vector<String> write_packet_vec;
    bool ready = false;
};

class TestMPPTunnelBase : public testing::Test
{
protected:
    virtual void SetUp() override { timeout = std::chrono::seconds(10); }
    virtual void TearDown() override {}
    std::chrono::seconds timeout;

public:
    MPPTunnelTestPtr constructRemoteSyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnelTest>(String("0000_0001"), timeout, 2, false, false, String("0"));
        return tunnel;
    }

    MPPTunnelTestPtr constructLocalSyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnelTest>(String("0000_0001"), timeout, 2, true, false, String("0"));
        return tunnel;
    }

    static MockLocalReaderPtr connectLocalSyncTunnel(MPPTunnelTestPtr mpp_tunnel_ptr)
    {
        mpp_tunnel_ptr->connect(nullptr);
        MockLocalReaderPtr local_reader_ptr = std::make_shared<MockLocalReader>(mpp_tunnel_ptr);
        mpp_tunnel_ptr->getThreadManager()->schedule(true, "LocalReader", [local_reader_ptr] {
            local_reader_ptr->read();
        });
        return local_reader_ptr;
    }

    MPPTunnelTestPtr constructRemoteAsyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnelTest>(String("0000_0001"), timeout, 2, false, true, String("0"));
        return tunnel;
    }
<<<<<<< HEAD
=======

    static void waitSyncTunnelSenderThread(SyncTunnelSenderPtr sync_tunnel_sender)
    {
        sync_tunnel_sender->thread_manager->wait();
    }

    static void setTunnelFinished(MPPTunnelPtr tunnel)
    {
        tunnel->status = MPPTunnel::TunnelStatus::Finished;
        if (tunnel->local_tunnel_v2)
            tunnel->local_tunnel_v2->is_done.store(true);
        else if (tunnel->local_tunnel_local_only_v2)
            tunnel->local_tunnel_local_only_v2->is_done.store(true);
    }

    static bool getTunnelConnectedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status != MPPTunnel::TunnelStatus::Unconnected
            && tunnel->status != MPPTunnel::TunnelStatus::Finished;
    }

    static bool getTunnelFinishedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status == MPPTunnel::TunnelStatus::Finished;
    }

    static bool getTunnelSenderConsumerFinishedFlag(TunnelSenderPtr sender) { return sender->isConsumerFinished(); }

    std::pair<MockExchangeReceiverPtr, std::vector<MPPTunnelPtr>> prepareLocal(const size_t tunnel_num)
    {
        MockExchangeReceiverPtr receiver = std::make_shared<MockExchangeReceiver>(tunnel_num);
        std::vector<MPPTunnelPtr> tunnels;
        for (size_t i = 0; i < tunnel_num; ++i)
            tunnels.push_back(constructLocalTunnel());

        receiver->connectLocalTunnel(tunnels);
        return std::pair<MockExchangeReceiverPtr, std::vector<MPPTunnelPtr>>(receiver, tunnels);
    }
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
};

TEST_F(TestMPPTunnelBase, ConnectWhenFinished)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->setFinishFlag(true);
    mpp_tunnel_ptr->connect(nullptr);
    GTEST_FAIL();
}
catch (Exception & e)
{
<<<<<<< HEAD
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has finished");
=======
    GTEST_ASSERT_EQ(
        e.message(),
        "Check status == TunnelStatus::Unconnected failed: MPPTunnel has connected or finished: Finished");
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

TEST_F(TestMPPTunnelBase, ConnectWhenConnected)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
<<<<<<< HEAD
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected");
=======
        GTEST_ASSERT_EQ(
            e.message(),
            "Check status == TunnelStatus::Unconnected failed: MPPTunnel has connected or finished: Connected");
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    }
}

TEST_F(TestMPPTunnelBase, CloseBeforeConnect)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), false);
}
CATCH

TEST_F(TestMPPTunnelBase, CloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
}
CATCH

<<<<<<< HEAD
TEST_F(TestMPPTunnelBase, ConnectWriteCancel)
=======
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
        GTEST_ASSERT_EQ(
            e.message(),
            "Check tunnel_sender != nullptr failed: write to tunnel 0000_0001 which is already closed.");
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
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 2); //Second for err msg
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, ConnectWriteWithCloseFlag)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr, true);
    mpp_tunnel_ptr->waitForFinish();
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, ConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
    auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, ConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
    auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->consumerFinish("");
    mpp_tunnel_ptr->getThreadManager()->wait();
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, WriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockFailedWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
<<<<<<< HEAD
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, grpc writes failed.");
=======
        GTEST_ASSERT_EQ(
            e.message(),
            "0000_0001: consumer exits unexpected, error message: 0000_0001 meet error: grpc writes failed. ");
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    }
}

TEST_F(TestMPPTunnelBase, WriteAfterFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->close("Canceled");
        auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
<<<<<<< HEAD
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
=======
        GTEST_ASSERT_EQ(
            e.message(),
            "0000_0001: consumer exits unexpected, error message: 0000_0001 meet error: grpc writes failed. ");
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    }
}

/// Test Local MPPTunnel
TEST_F(TestMPPTunnelBase, LocalConnectWhenFinished)
try
{
<<<<<<< HEAD
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->setFinishFlag(true);
    mpp_tunnel_ptr->connect(nullptr);
=======
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
    GTEST_ASSERT_EQ(receiver->getReceivedMsgs()[0]->getPacket().data(), "First");
    GTEST_ASSERT_EQ(receiver->getReceivedMsgs()[1]->getPacket().data(), "Second");
}
CATCH

TEST_F(TestMPPTunnel, LocalConnectWhenFinished)
try
{
    auto [receiver, tunnels] = prepareLocal(1);
    setTunnelFinished(tunnels[0]);
    ReceivedMessageQueue received_message_queue(1, Logger::get(), nullptr, false, 0);

    LocalRequestHandler local_req_handler([](bool, const String &) {}, []() {}, []() {}, "", &received_message_queue);
    tunnels[0]->connectLocalV2(0, local_req_handler, false);
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    GTEST_FAIL();
}
catch (Exception & e)
{
<<<<<<< HEAD
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has finished");
=======
    GTEST_ASSERT_EQ(
        e.message(),
        "Check status == TunnelStatus::Unconnected failed: MPPTunnel 0000_0001 has connected or finished: Finished");
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

TEST_F(TestMPPTunnelBase, LocalConnectWhenConnected)
{
<<<<<<< HEAD
=======
    auto [receiver, tunnels] = prepareLocal(1);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(tunnels[0]), true);
    ReceivedMessageQueue queue(1, Logger::get(), nullptr, false, 0);
    LocalRequestHandler local_req_handler([](bool, const String &) {}, []() {}, []() {}, "", &queue);
    tunnels[0]->connectLocalV2(0, local_req_handler, false);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(
        e.message(),
        "Check status == TunnelStatus::Unconnected failed: MPPTunnel 0000_0001 has connected or finished: Connected");
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
    GTEST_ASSERT_EQ(
        e.message(),
        "Check tunnel_sender != nullptr failed: write to tunnel 0000_0001 which is already closed.");
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
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->connect(nullptr);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected");
    }
}

TEST_F(TestMPPTunnelBase, LocalCloseBeforeConnect)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), false);
}
CATCH

TEST_F(TestMPPTunnelBase, LocalCloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
}
CATCH

TEST_F(TestMPPTunnelBase, LocalConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    mpp_tunnel_ptr->getThreadManager()->wait(); // Join local read thread
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 2); //Second for err msg
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, LocalConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    mpp_tunnel_ptr->getThreadManager()->wait(); // Join local read thread
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
    LOG_FMT_TRACE(mpp_tunnel_ptr->getLog(), "basic logic done!");
}
CATCH

TEST_F(TestMPPTunnelBase, LocalConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->consumerFinish("");
    mpp_tunnel_ptr->getThreadManager()->wait();
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, LocalReadTerminate)
{
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        mpp_tunnel_ptr->connect(nullptr);
        MockTerminateLocalReaderPtr local_reader_ptr = std::make_shared<MockTerminateLocalReader>(mpp_tunnel_ptr);
        mpp_tunnel_ptr->getThreadManager()->schedule(true, "LocalReader", [local_reader_ptr] {
            local_reader_ptr->read();
        });
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, Receiver closed");
    }
}

TEST_F(TestMPPTunnelBase, LocalWriteAfterFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->close("");
        std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
    }
}

/// Test Async MPPTunnel
TEST_F(TestMPPTunnelBase, AsyncConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connect(async_writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    data_packet_ptr->set_data("Second");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 3); //Third for err msg
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[0], "First");
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[1], "Second");
}
CATCH

TEST_F(TestMPPTunnelBase, AsyncConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connect(async_writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnelBase, AsyncConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connect(async_writer_ptr.get());
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->consumerFinish("");
    GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 0);
}
CATCH

TEST_F(TestMPPTunnelBase, AsyncWriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
        std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockFailedAsyncWriter>(mpp_tunnel_ptr);
        mpp_tunnel_ptr->connect(async_writer_ptr.get());
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        data_packet_ptr->set_data("Second");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, grpc writes failed.");
    }
}

} // namespace tests
} // namespace DB

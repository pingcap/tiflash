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
    bool write(const mpp::MPPDataPacket &) override
    {
        return false;
    }
};

struct MockLocalReader
{
    LocalTunnelSenderPtr local_sender;
    std::vector<String> write_packet_vec;
    std::shared_ptr<ThreadManager> thread_manager;

    explicit MockLocalReader(const LocalTunnelSenderPtr & local_sender_)
        : local_sender(local_sender_)
        , thread_manager(newThreadManager())
    {
        thread_manager->schedule(true, "LocalReader", [this] {
            this->read();
        });
    }

    ~MockLocalReader()
    {
        if (local_sender)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            LOG_FMT_TRACE(local_sender->getLogger(), "before mocklocalreader invoking consumerFinish!");
            local_sender->consumerFinish("Receiver closed");
            LOG_FMT_TRACE(local_sender->getLogger(), "after mocklocalreader invoking consumerFinish!");
        }
        thread_manager->wait();
    }

    void read()
    {
        while (true)
        {
            TrackedMppDataPacketPtr tmp_packet = local_sender->readForLocal();
            bool success = tmp_packet != nullptr;
            if (success)
            {
                write_packet_vec.push_back(tmp_packet->packet.data());
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
    LocalTunnelSenderPtr local_sender;
    std::shared_ptr<ThreadManager> thread_manager;

    explicit MockTerminateLocalReader(const LocalTunnelSenderPtr & local_sender_)
        : local_sender(local_sender_)
        , thread_manager(newThreadManager())
    {
        thread_manager->schedule(true, "LocalReader", [this] {
            this->read();
        });
    }

    ~MockTerminateLocalReader()
    {
        if (local_sender)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            local_sender->consumerFinish("Receiver closed");
        }
        thread_manager->wait();
    }

    void read() const
    {
        TrackedMppDataPacketPtr tmp_packet = local_sender->readForLocal();
        local_sender->consumerFinish("Receiver closed");
    }
};
using MockTerminateLocalReaderPtr = std::shared_ptr<MockTerminateLocalReader>;


class MockAsyncWriter : public PacketWriter
{
public:
    explicit MockAsyncWriter() {}

    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        // Simulate the async process, write success then check if exist msg, then write again
        if (async_sender->isSendQueueNextPopNonBlocking())
        {
            async_sender->sendOne();
        }
        return true;
    }

    void tryFlushOne() override
    {
        if (ready && async_sender->isSendQueueNextPopNonBlocking())
        {
            async_sender->sendOne();
        }
        ready = true;
    }
    void attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> & async_tunnel_sender_) override
    {
        async_sender = async_tunnel_sender_;
    }
    AsyncTunnelSenderPtr async_sender;
    std::vector<String> write_packet_vec;
    bool ready = false;
};

class MockFailedAsyncWriter : public PacketWriter
{
public:
    explicit MockFailedAsyncWriter() {}
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        // Simulate the async process, write success then check if exist msg, then write again
        if (async_sender->isSendQueueNextPopNonBlocking())
        {
            async_sender->sendOne();
        }
        return false;
    }

    void tryFlushOne() override
    {
        if (ready && async_sender->isSendQueueNextPopNonBlocking())
        {
            async_sender->sendOne();
        }
        ready = true;
    }

    void attachAsyncTunnelSender(const std::shared_ptr<DB::AsyncTunnelSender> & async_tunnel_sender_) override
    {
        async_sender = async_tunnel_sender_;
    }
    AsyncTunnelSenderPtr async_sender;
    std::vector<String> write_packet_vec;
    bool ready = false;
};

class TestMPPTunnel : public testing::Test
{
protected:
    virtual void SetUp() override { timeout = std::chrono::seconds(10); }
    virtual void TearDown() override {}
    std::chrono::seconds timeout;

public:
    MPPTunnelPtr constructRemoteSyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, false, false, String("0"));
        return tunnel;
    }

    MPPTunnelPtr constructLocalSyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, true, false, String("0"));
        return tunnel;
    }

    static MockLocalReaderPtr connectLocalSyncTunnel(MPPTunnelPtr mpp_tunnel_ptr)
    {
        mpp_tunnel_ptr->connect(nullptr);
        MockLocalReaderPtr local_reader_ptr = std::make_shared<MockLocalReader>(mpp_tunnel_ptr->getLocalTunnelSender());
        return local_reader_ptr;
    }

    MPPTunnelPtr constructRemoteAsyncTunnel()
    {
        auto tunnel = std::make_shared<MPPTunnel>(String("0000_0001"), timeout, 2, false, true, String("0"));
        return tunnel;
    }

    void waitSyncTunnelSenderThread(SyncTunnelSenderPtr sync_tunnel_sender)
    {
        sync_tunnel_sender->thread_manager->wait();
    }

    void setTunnelFinished(MPPTunnelPtr tunnel)
    {
        tunnel->status = MPPTunnel::TunnelStatus::Finished;
    }

    bool getTunnelConnectedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status != MPPTunnel::TunnelStatus::Unconnected && tunnel->status != MPPTunnel::TunnelStatus::Finished;
    }

    bool getTunnelFinishedFlag(MPPTunnelPtr tunnel)
    {
        return tunnel->status == MPPTunnel::TunnelStatus::Finished;
    }

    bool getTunnelSenderConsumerFinishedFlag(TunnelSenderPtr sender)
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
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
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
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), false);
}
CATCH

TEST_F(TestMPPTunnel, CloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
}
CATCH

TEST_F(TestMPPTunnel, WriteAfterUnconnectFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        setTunnelFinished(mpp_tunnel_ptr);
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
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
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
    }
}

TEST_F(TestMPPTunnel, ConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 2); //Second for err msg
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, ConnectWriteWithCloseFlag)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr, true);
    mpp_tunnel_ptr->waitForFinish();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, ConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, ConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
    std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
    mpp_tunnel_ptr->connect(writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
    auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->getSyncTunnelSender()->consumerFinish("");
    waitSyncTunnelSenderThread(mpp_tunnel_ptr->getSyncTunnelSender());

    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr.get())->write_packet_vec[0], "First");
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
        auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
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
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        std::unique_ptr<PacketWriter> writer_ptr = std::make_unique<MockWriter>();
        mpp_tunnel_ptr->connect(writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->close("Canceled");
        auto data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
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

/// Test Local MPPTunnel
TEST_F(TestMPPTunnel, LocalConnectWhenFinished)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    setTunnelFinished(mpp_tunnel_ptr);
    mpp_tunnel_ptr->connect(nullptr);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected or finished: Finished");
}

TEST_F(TestMPPTunnel, LocalConnectWhenConnected)
{
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
        mpp_tunnel_ptr->connect(nullptr);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected or finished: Connected");
    }
}

TEST_F(TestMPPTunnel, LocalCloseBeforeConnect)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), false);
}
CATCH

TEST_F(TestMPPTunnel, LocalCloseAfterClose)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    mpp_tunnel_ptr->close("Canceled");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
}
CATCH

TEST_F(TestMPPTunnel, LocalConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    local_reader_ptr->thread_manager->wait(); // Join local read thread
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 2); //Second for err msg
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, LocalConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    local_reader_ptr->thread_manager->wait(); // Join local read thread
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
    LOG_FMT_TRACE(mpp_tunnel_ptr->getLogger(), "basic logic done!");
}
CATCH

TEST_F(TestMPPTunnel, LocalConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->getTunnelSender()->consumerFinish("");
    local_reader_ptr->thread_manager->wait(); // Join local read thread
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(local_reader_ptr->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, LocalReadTerminate)
{
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        mpp_tunnel_ptr->connect(nullptr);
        MockTerminateLocalReaderPtr local_reader_ptr = std::make_shared<MockTerminateLocalReader>(mpp_tunnel_ptr->getLocalTunnelSender());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
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

TEST_F(TestMPPTunnel, LocalWriteAfterFinished)
{
    try
    {
        auto mpp_tunnel_ptr = constructLocalSyncTunnel();
        auto local_reader_ptr = connectLocalSyncTunnel(mpp_tunnel_ptr);
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
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
TEST_F(TestMPPTunnel, AsyncConnectWriteCancel)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>();
    mpp_tunnel_ptr->connect(async_writer_ptr.get());

    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    data_packet_ptr->set_data("Second");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->close("Cancel");
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 3); //Third for err msg
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[0], "First");
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[1], "Second");
}
CATCH

TEST_F(TestMPPTunnel, AsyncConnectWriteWriteDone)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>();
    mpp_tunnel_ptr->connect(async_writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->writeDone();
    GTEST_ASSERT_EQ(getTunnelFinishedFlag(mpp_tunnel_ptr), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 1);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec[0], "First");
}
CATCH

TEST_F(TestMPPTunnel, AsyncConsumerFinish)
try
{
    auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
    std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockAsyncWriter>();
    mpp_tunnel_ptr->connect(async_writer_ptr.get());
    GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);

    std::unique_ptr<mpp::MPPDataPacket> data_packet_ptr = std::make_unique<mpp::MPPDataPacket>();
    data_packet_ptr->set_data("First");
    mpp_tunnel_ptr->write(*data_packet_ptr);
    mpp_tunnel_ptr->getTunnelSender()->consumerFinish("");
    GTEST_ASSERT_EQ(getTunnelSenderConsumerFinishedFlag(mpp_tunnel_ptr->getTunnelSender()), true);
    GTEST_ASSERT_EQ(dynamic_cast<MockAsyncWriter *>(async_writer_ptr.get())->write_packet_vec.size(), 0);
}
CATCH

TEST_F(TestMPPTunnel, AsyncWriteError)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteAsyncTunnel();
        std::unique_ptr<PacketWriter> async_writer_ptr = std::make_unique<MockFailedAsyncWriter>();
        mpp_tunnel_ptr->connect(async_writer_ptr.get());
        GTEST_ASSERT_EQ(getTunnelConnectedFlag(mpp_tunnel_ptr), true);
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
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, 0000_0001 meet error: grpc writes failed.");
    }
}

} // namespace tests
} // namespace DB
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
    bool write(const mpp::MPPDataPacket &) override
    {
        return false;
    }
};

struct MockLocalReader
{
    MPPTunnelTestPtr tunnel;
    std::vector<String> write_packet_vec;

    explicit MockLocalReader(const MPPTunnelTestPtr & tunnel_)
        : tunnel(tunnel_)
    {}

    ~MockLocalReader()
    {
        if (tunnel)
        {
            // In case that ExchangeReceiver throw error before finish reading from mpp_tunnel
            LOG_FMT_TRACE(tunnel->getLog(), "before mocklocalreader invoking consumerFinish!");
            tunnel->consumerFinish("Receiver closed");
            LOG_FMT_TRACE(tunnel->getLog(), "after mocklocalreader invoking consumerFinish!");
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
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has finished");
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
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected");
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

TEST_F(TestMPPTunnelBase, ConnectWriteCancel)
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
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, grpc writes failed.");
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
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
    }
}

/// Test Local MPPTunnel
TEST_F(TestMPPTunnelBase, LocalConnectWhenFinished)
try
{
    auto mpp_tunnel_ptr = constructLocalSyncTunnel();
    mpp_tunnel_ptr->setFinishFlag(true);
    mpp_tunnel_ptr->connect(nullptr);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "MPPTunnel has finished");
}

TEST_F(TestMPPTunnelBase, LocalConnectWhenConnected)
{
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

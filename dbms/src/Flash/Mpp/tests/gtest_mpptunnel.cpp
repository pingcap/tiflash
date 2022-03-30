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
#include <Flash/Mpp/MPPTunnel.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <iostream>
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
};

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
    bool write(const mpp::MPPDataPacket & packet) override
    {
        write_packet_vec.push_back(packet.data());
        return false;
    }

public:
    std::vector<String> write_packet_vec;
};

class TestMPPTunnelBase : public testing::Test
{
protected:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

public:
    using MPPTunnelTestPtr = std::shared_ptr<MPPTunnelTest>;
    MPPTunnelTestPtr constructRemoteSyncTunnel()
    {
        std::chrono::seconds timeout(10);
        auto tunnel = std::make_shared<MPPTunnelTest>(String("0000_0001"), timeout, 2, false, false, String("0"));
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
    PacketWriter * writer_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        GTEST_ASSERT_EQ(e.message(), "MPPTunnel has connected");
    }
}

TEST_F(TestMPPTunnelBase, CloseBeforeConnect)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        mpp_tunnel_ptr->close("Canceled");
    }
    catch (Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        GTEST_FAIL();
    }
}

TEST_F(TestMPPTunnelBase, CloseAfterClose)
{
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        mpp_tunnel_ptr->close("Canceled");
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
        mpp_tunnel_ptr->close("Canceled");
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
    }
    catch (Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        GTEST_FAIL();
    }
}

TEST_F(TestMPPTunnelBase, ConnectWriteClose)
{
    PacketWriter * writer_ptr = nullptr;
    mpp::MPPDataPacket * data_packet_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        data_packet_ptr = new mpp::MPPDataPacket();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // wait for sendJob thread to write data
        mpp_tunnel_ptr->close("Canceled");
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec.size(), 2); //Second for err msg
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec[0], "First");

        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
        std::cerr << e.displayText() << std::endl;
        GTEST_FAIL();
    }
}

TEST_F(TestMPPTunnelBase, ConnectWriteWriteDone)
{
    PacketWriter * writer_ptr = nullptr;
    mpp::MPPDataPacket * data_packet_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        data_packet_ptr = new mpp::MPPDataPacket();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // wait for sendJob thread to write data
        mpp_tunnel_ptr->writeDone();
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec.size(), 1);
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec[0], "First");

        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
        std::cerr << e.displayText() << std::endl;
        GTEST_FAIL();
    }
}

TEST_F(TestMPPTunnelBase, ConsumerFinish)
{
    PacketWriter * writer_ptr = nullptr;
    mpp::MPPDataPacket * data_packet_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        data_packet_ptr = new mpp::MPPDataPacket();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // wait for sendJob thread to write data
        mpp_tunnel_ptr->consumerFinish("");
        mpp_tunnel_ptr->getThreadManager()->wait();
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getFinishFlag(), true);
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec.size(), 1);
        GTEST_ASSERT_EQ(dynamic_cast<MockWriter *>(writer_ptr)->write_packet_vec[0], "First");
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
        GTEST_FAIL();
    }
}

TEST_F(TestMPPTunnelBase, WriteError)
{
    PacketWriter * writer_ptr = nullptr;
    mpp::MPPDataPacket * data_packet_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockFailedWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        data_packet_ptr = new mpp::MPPDataPacket();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
        GTEST_ASSERT_EQ(e.message(), "Consumer exits unexpected, grpc writes failed.");
    }
}

TEST_F(TestMPPTunnelBase, WriteAfterFinished)
{
    PacketWriter * writer_ptr = nullptr;
    mpp::MPPDataPacket * data_packet_ptr = nullptr;
    try
    {
        auto mpp_tunnel_ptr = constructRemoteSyncTunnel();
        writer_ptr = new MockWriter();
        mpp_tunnel_ptr->connect(writer_ptr);
        GTEST_ASSERT_EQ(mpp_tunnel_ptr->getConnectFlag(), true);
        mpp_tunnel_ptr->close("Canceled");
        data_packet_ptr = new mpp::MPPDataPacket();
        data_packet_ptr->set_data("First");
        mpp_tunnel_ptr->write(*data_packet_ptr);
        mpp_tunnel_ptr->waitForFinish();
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        if (writer_ptr)
            delete writer_ptr;
        if (data_packet_ptr)
            delete data_packet_ptr;
        GTEST_ASSERT_EQ(e.message(), "write to tunnel which is already closed,");
    }
}

} // namespace tests
} // namespace DB

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

#pragma once

#include <Common/DynamicThreadPool.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/tests/WindowTestUtil.h>
#include <Interpreters/Join.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <WindowFunctions/registerWindowFunctions.h>
#include <benchmark/benchmark.h>

#include <random>

namespace DB
{
namespace tests
{


using Packet = mpp::MPPDataPacket;
using PacketPtr = std::shared_ptr<Packet>;
using PacketQueue = MPMCQueue<PacketPtr>;
using PacketQueuePtr = std::shared_ptr<PacketQueue>;
using StopFlag = std::atomic<bool>;

// NOLINTBEGIN(readability-convert-member-functions-to-static)
struct MockReceiverContext
{
    using Status = ::grpc::Status;
    struct Request
    {
        String debugString() const
        {
            return "{Request}";
        }

        int source_index = 0;
        int send_task_id = 0;
        int recv_task_id = -1;
    };

    struct Reader
    {
        explicit Reader(const PacketQueuePtr & queue_)
            : queue(queue_)
        {}

        void initialize() const
        {
        }

        bool read(PacketPtr & packet [[maybe_unused]]) const
        {
            PacketPtr res;
            if (queue->pop(res))
            {
                *packet = *res; // avoid change shared packets
                return true;
            }
            return false;
        }

        Status finish() const
        {
            return ::grpc::Status();
        }

        PacketQueuePtr queue;
    };

    struct MockAsyncGrpcExchangePacketReader
    {
        // Not implement benchmark for Async GRPC for now.
        void init(UnaryCallback<bool> *) { assert(0); }
        void read(TrackedMppDataPacketPtr &, UnaryCallback<bool> *) { assert(0); }
        void finish(::grpc::Status &, UnaryCallback<bool> *) { assert(0); }
    };

    using AsyncReader = MockAsyncGrpcExchangePacketReader;

    MockReceiverContext(
        const std::vector<PacketQueuePtr> & queues_,
        const std::vector<tipb::FieldType> & field_types_)
        : queues(queues_)
        , field_types(field_types_)
    {
    }

    void fillSchema(DAGSchema & schema) const
    {
        schema.clear();
        for (size_t i = 0; i < field_types.size(); ++i)
        {
            String name = "exchange_receiver_" + std::to_string(i);
            ColumnInfo info = TiDB::fieldTypeToColumnInfo(field_types[i]);
            schema.emplace_back(std::move(name), std::move(info));
        }
    }

    Request makeRequest(int index) const
    {
        return {index, index, -1};
    }

    std::shared_ptr<Reader> makeReader(const Request & request)
    {
        return std::make_shared<Reader>(queues[request.send_task_id]);
    }

    static Status getStatusOK()
    {
        return ::grpc::Status();
    }

    bool supportAsync(const Request &) const { return false; }
    void makeAsyncReader(
        const Request &,
        std::shared_ptr<AsyncReader> &,
        grpc::CompletionQueue *,
        UnaryCallback<bool> *) const {}

    std::vector<PacketQueuePtr> queues;
    std::vector<tipb::FieldType> field_types;
};
// NOLINTEND(readability-convert-member-functions-to-static)

using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;
using MockExchangeReceiverPtr = std::shared_ptr<MockExchangeReceiver>;
using MockExchangeReceiverInputStream = TiRemoteBlockInputStream<MockExchangeReceiver>;

struct MockWriter : public PacketWriter
{
    explicit MockWriter(PacketQueuePtr queue_)
        : queue(std::move(queue_))
    {}

    bool write(const Packet & packet) override
    {
        queue->push(std::make_shared<Packet>(packet));
        return true;
    }

    void finish()
    {
        queue->finish();
    }

    PacketQueuePtr queue;
};

using MockWriterPtr = std::shared_ptr<MockWriter>;
using MockTunnel = MPPTunnelBase<MockWriter>;
using MockTunnelPtr = std::shared_ptr<MockTunnel>;
using MockTunnelSet = MPPTunnelSetBase<MockTunnel>;
using MockTunnelSetPtr = std::shared_ptr<MockTunnelSet>;

struct MockBlockInputStream : public IProfilingBlockInputStream
{
    const std::vector<Block> & blocks;
    Block header;
    std::mt19937 mt;
    std::uniform_int_distribution<int> dist;
    StopFlag & stop_flag;

    MockBlockInputStream(const std::vector<Block> & blocks_, StopFlag & stop_flag_);

    String getName() const override { return "MockBlockInputStream"; }
    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (stop_flag.load(std::memory_order_relaxed))
            return Block{};
        return blocks[dist(mt)];
    }
};

// Similar to MockBlockInputStream, but return fixed count of rows.
struct MockFixedRowsBlockInputStream : public IProfilingBlockInputStream
{
    Block header;
    std::mt19937 mt;
    std::uniform_int_distribution<int> dist;
    size_t current_rows;
    size_t total_rows;
    const std::vector<Block> & blocks;

    MockFixedRowsBlockInputStream(size_t total_rows_, const std::vector<Block> & blocks_);

    String getName() const override { return "MockBlockInputStream"; }
    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (current_rows >= total_rows)
            return Block{};
        Block res = blocks[dist(mt)];
        current_rows += res.rows();
        return res;
    }
};

Block makeBlock(int row_num, bool skew = false);
std::vector<Block> makeBlocks(int block_num, int row_num, bool skew = false);
mpp::MPPDataPacket makePacket(ChunkCodecStream & codec, int row_num);
std::vector<PacketPtr> makePackets(ChunkCodecStream & codec, int packet_num, int row_num);
std::vector<PacketQueuePtr> makePacketQueues(int source_num, int queue_size);
std::vector<tipb::FieldType> makeFields();
void printException(const Exception & e);
void sendPacket(const std::vector<PacketPtr> & packets, const PacketQueuePtr & queue, StopFlag & stop_flag);
void receivePacket(const PacketQueuePtr & queue);

struct ReceiverHelper
{
    const int concurrency;
    const int source_num;
    const uint32_t fine_grained_shuffle_stream_count;
    tipb::ExchangeReceiver pb_exchange_receiver;
    std::vector<tipb::FieldType> fields;
    mpp::TaskMeta task_meta;
    std::vector<PacketQueuePtr> queues;
    std::shared_ptr<Join> join_ptr;

    explicit ReceiverHelper(int concurrency_, int source_num_, uint32_t fine_grained_shuffle_stream_count_);
    MockExchangeReceiverPtr buildReceiver();
    std::vector<BlockInputStreamPtr> buildExchangeReceiverStream();
    BlockInputStreamPtr buildUnionStream();
    void finish();
};

struct SenderHelper
{
    const int source_num;
    const int concurrency;
    const uint32_t fine_grained_shuffle_stream_count;
    const int64_t fine_grained_shuffle_batch_size;

    std::vector<PacketQueuePtr> queues;
    std::vector<MockWriterPtr> mock_writers;
    std::vector<MockTunnelPtr> tunnels;
    MockTunnelSetPtr tunnel_set;
    std::unique_ptr<DAGContext> dag_context;

    SenderHelper(
        int source_num_,
        int concurrency_,
        uint32_t fine_grained_shuffle_stream_count_,
        int64_t fine_grained_shuffle_batch_size_,
        const std::vector<PacketQueuePtr> & queues_,
        const std::vector<tipb::FieldType> & fields);

    // Using MockBlockInputStream to build streams.
    BlockInputStreamPtr buildUnionStream(StopFlag & stop_flag, const std::vector<Block> & blocks);
    // Using MockFixedRowsBlockInputStream to build streams.
    BlockInputStreamPtr buildUnionStream(size_t total_rows, const std::vector<Block> & blocks);

    void finish();
};

class ExchangeBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override;
    void TearDown(const benchmark::State &) override;
    void runAndWait(std::shared_ptr<ReceiverHelper> receiver_helper,
                    BlockInputStreamPtr receiver_stream,
                    std::shared_ptr<SenderHelper> & sender_helper,
                    BlockInputStreamPtr sender_stream);

    std::vector<Block> uniform_blocks;
    std::vector<Block> skew_blocks;
};


} // namespace tests
} // namespace DB

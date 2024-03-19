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

#include <DataStreams/SquashingTransform.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Schema/TiDB.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>

#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.cpp>
#include <Flash/Mpp/ExchangeReceiver.cpp>
#include <memory>
#include <utility>


namespace DB
{
namespace tests
{
using Packet = TrackedMppDataPacket;
using PacketPtr = std::shared_ptr<Packet>;
using PacketQueue = MPMCQueue<PacketPtr>;
using PacketQueuePtr = std::shared_ptr<PacketQueue>;

bool equalSummaries(const ExecutionSummary & left, const ExecutionSummary & right)
{
    /// We only sampled some fields to compare equality in this test.
    /// It would be better to check all fields.
    /// This can be done by using C++20's default comparsion feature when we switched to use C++20:
    /// https://en.cppreference.com/w/cpp/language/default_comparisons
    return (left.concurrency == right.concurrency) && //
        (left.num_iterations == right.num_iterations) && //
        (left.num_produced_rows == right.num_produced_rows) && //
        (left.time_processed_ns == right.time_processed_ns) && //
        (left.scan_context->dmfile_data_scanned_rows == right.scan_context->dmfile_data_scanned_rows) && //
        (left.scan_context->dmfile_data_skipped_rows == right.scan_context->dmfile_data_skipped_rows);
}

struct MockWriter
{
    MockWriter(DAGContext & dag_context, PacketQueuePtr queue_)
        : result_field_types(dag_context.result_field_types)
        , queue(queue_)
    {}

    static ExecutionSummary mockExecutionSummary()
    {
        ExecutionSummary summary;
        summary.time_processed_ns = 100;
        summary.num_produced_rows = 10000;
        summary.num_iterations = 50;
        summary.concurrency = 1;
        summary.scan_context = std::make_unique<DM::ScanContext>();

        summary.scan_context->dmfile_data_scanned_rows = 8000;
        summary.scan_context->dmfile_data_skipped_rows = 15000;
        summary.scan_context->dmfile_mvcc_scanned_rows = 8000;
        summary.scan_context->dmfile_mvcc_skipped_rows = 15000;
        summary.scan_context->dmfile_lm_filter_scanned_rows = 8000;
        summary.scan_context->dmfile_lm_filter_skipped_rows = 15000;
        summary.scan_context->total_dmfile_rough_set_index_check_time_ns = 10;
        summary.scan_context->total_dmfile_read_time_ns = 200;
        summary.scan_context->create_snapshot_time_ns = 5;
        summary.scan_context->total_local_region_num = 10;
        summary.scan_context->total_remote_region_num = 5;
        return summary;
    }

    void broadcastOrPassThroughWriteV0(Blocks & blocks)
    {
        auto && packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
        ++total_packets;
        if (!packet)
            return;

        total_bytes += packet->packet.ByteSizeLong();
        queue->push(std::move(packet));
    }

    void broadcastWrite(Blocks & blocks) { return broadcastOrPassThroughWriteV0(blocks); }
    void passThroughWrite(Blocks & blocks) { return broadcastOrPassThroughWriteV0(blocks); }
    void broadcastOrPassThroughWrite(
        Blocks & blocks,
        MPPDataPacketVersion version,
        CompressionMethod compression_method)
    {
        if (version == MPPDataPacketV0)
            return broadcastOrPassThroughWriteV0(blocks);

        size_t original_size{};
        auto && packet = MPPTunnelSetHelper::ToPacket(std::move(blocks), version, compression_method, original_size);
        ++total_packets;
        if (!packet)
            return;

        total_bytes += packet->packet.ByteSizeLong();
        queue->push(std::move(packet));
    }
    void broadcastWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
    {
        return broadcastOrPassThroughWrite(blocks, version, compression_method);
    }
    void passThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
    {
        return broadcastOrPassThroughWrite(blocks, version, compression_method);
    }

    void write(tipb::SelectResponse & response)
    {
        if (add_summary)
        {
            auto * summary_ptr = response.add_execution_summaries();
            auto summary = mockExecutionSummary();
            summary_ptr->set_time_processed_ns(summary.time_processed_ns);
            summary_ptr->set_num_produced_rows(summary.num_produced_rows);
            summary_ptr->set_num_iterations(summary.num_iterations);
            summary_ptr->set_concurrency(summary.concurrency);
            summary_ptr->mutable_tiflash_scan_context()->CopyFrom(summary.scan_context->serialize());
            summary_ptr->set_executor_id("Executor_0");
        }
        ++total_packets;
        if (!response.chunks().empty())
            total_bytes += response.ByteSizeLong();
        mpp::MPPDataPacket packet;
        auto tracked_packet = std::make_shared<TrackedMppDataPacket>(packet, nullptr);
        tracked_packet->serializeByResponse(response);
        queue->push(tracked_packet);
    }
    static uint16_t getPartitionNum() { return 1; }
    static bool isWritable() { throw Exception("Unsupport async write"); }

    std::vector<tipb::FieldType> result_field_types;

    PacketQueuePtr queue;
    bool add_summary = false;
    size_t total_packets = 0;
    size_t total_bytes = 0;
};

// NOLINTBEGIN(readability-convert-member-functions-to-static)
struct MockReceiverContext
{
    using Status = ::grpc::Status;
    struct Request
    {
        static String debugString() { return "{Request}"; }

        int source_index = 0;
        int send_task_id = 0;
        int recv_task_id = -1;
        bool is_local = false;
    };

    struct MockClientContext
    {
        static grpc_call * c_call() { return nullptr; }
    };

    struct Reader
    {
        explicit Reader(const PacketQueuePtr & queue_)
            : queue(queue_)
        {}

        void initialize() const {}

        bool read(PacketPtr & packet [[maybe_unused]]) const
        {
            PacketPtr res;
            if (queue->pop(res) == MPMCQueueResult::OK)
            {
                *packet = *res; // avoid change shared packets
                return true;
            }
            return false;
        }

        static Status finish() { return ::grpc::Status(); }

        void cancel(const String &) {}

        PacketQueuePtr queue;
    };

    struct MockAsyncGrpcExchangePacketReader
    {
        // Not implement benchmark for Async GRPC for now.
        static void init(GRPCKickTag *) { assert(0); }
        static void read(TrackedMppDataPacketPtr &, GRPCKickTag *) { assert(0); }
        static void finish(::grpc::Status &, GRPCKickTag *) { assert(0); }
        static std::shared_ptr<MockClientContext> getClientContext() { return std::make_shared<MockClientContext>(); }
    };

    using AsyncReader = MockAsyncGrpcExchangePacketReader;

    MockReceiverContext(PacketQueuePtr & queue_, const std::vector<tipb::FieldType> & field_types_)
        : queue(queue_)
        , field_types(field_types_)
    {}

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

    static Request makeRequest(int index) { return {index, index, -1}; }

    std::unique_ptr<Reader> makeSyncReader(const Request &) { return std::make_unique<Reader>(queue); }

    static Status getStatusOK() { return ::grpc::Status(); }

    static bool supportAsync(const Request &) { return false; }

    static std::unique_ptr<AsyncReader> makeAsyncReader(const Request &, grpc::CompletionQueue *, GRPCKickTag *)
    {
        return nullptr;
    }

    std::shared_ptr<Reader> makeReader(const Request &) { return std::make_shared<Reader>(queue); }

    static std::tuple<MPPTunnelPtr, grpc::Status> establishMPPConnectionLocalV1(
        const ::mpp::EstablishMPPConnectionRequest *,
        const std::shared_ptr<MPPTaskManager> &)
    {
        // Useless, just for compilation
        return std::make_pair(MPPTunnelPtr(), grpc::Status::CANCELLED);
    }

    void establishMPPConnectionLocalV2(const Request &, size_t, LocalRequestHandler &, bool) {}

    PacketQueuePtr queue;
    std::vector<tipb::FieldType> field_types{};
};
using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;
using MockWriterPtr = std::shared_ptr<MockWriter>;
using MockExchangeReceiverInputStream = TiRemoteBlockInputStream<MockExchangeReceiver>;

class TestTiRemoteBlockInputStream : public testing::Test
{
protected:
    void SetUp() override
    {
        context = TiFlashTestEnv::getContext();
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        dag_context_ptr->kind = DAGRequestKind::MPP;
        dag_context_ptr->is_root_mpp_task = true;
        dag_context_ptr->result_field_types = makeFields();
        dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
    }

public:
    TestTiRemoteBlockInputStream() = default;

    static Block squashBlocks(std::vector<Block> & blocks)
    {
        std::vector<Block> reference_block_vec;
        SquashingTransform squash_transform(std::numeric_limits<UInt64>::max(), 0, "");
        for (auto & block : blocks)
            squash_transform.add(std::move(block));
        Block empty;
        auto result = squash_transform.add(std::move(empty));
        return result.block;
    }

    // Return 10 Int64 column.
    static std::vector<tipb::FieldType> makeFields()
    {
        std::vector<tipb::FieldType> fields(10);
        for (int i = 0; i < 10; ++i)
        {
            fields[i].set_tp(TiDB::TypeLongLong);
            fields[i].set_flag(TiDB::ColumnFlagNotNull);
        }
        return fields;
    }

    static DAGSchema makeSchema()
    {
        auto fields = makeFields();
        DAGSchema schema;
        for (size_t i = 0; i < fields.size(); ++i)
        {
            ColumnInfo info = TiDB::fieldTypeToColumnInfo(fields[i]);
            schema.emplace_back(String("col") + std::to_string(i), std::move(info));
        }
        return schema;
    }

    // Return a block with **rows** and 10 Int64 column.
    static Block prepareBlock(size_t rows)
    {
        Block block;
        for (size_t i = 0; i < 10; ++i)
        {
            DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
            auto int64_column = ColumnGenerator::instance().generate({rows, "Int64", RANDOM}).column;
            block.insert(
                ColumnWithTypeAndName{std::move(int64_column), int64_data_type, String("col") + std::to_string(i)});
        }
        return block;
    }

    static void prepareBlocks(std::vector<Block> & source_blocks, bool empty_last_packet)
    {
        const size_t block_rows = 8192 * 3 / 8;
        /// 61 is specially chosen so that the last packet contains 3072 rows, and then end of the queue
        size_t block_num = empty_last_packet ? 60 : 61;
        // 1. Build Blocks.
        for (size_t i = 0; i < block_num; ++i)
            source_blocks.emplace_back(prepareBlock(block_rows));
    }

    void prepareQueue(std::shared_ptr<MockWriter> & writer, std::vector<Block> & source_blocks, bool empty_last_packet)
        const
    {
        prepareBlocks(source_blocks, empty_last_packet);

        const size_t batch_send_min_limit = 4096;
        auto dag_writer = std::make_shared<BroadcastOrPassThroughWriter<MockWriterPtr>>(
            writer,
            batch_send_min_limit,
            *dag_context_ptr,
            MPPDataPacketVersion::MPPDataPacketV1,
            tipb::CompressionMode::FAST,
            tipb::ExchangeType::Broadcast);

        // 2. encode all blocks
        for (const auto & block : source_blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 3. send execution summary
        writer->add_summary = true;
        tipb::SelectResponse summary_response;
        writer->write(summary_response);
    }

    void prepareQueueV2(
        std::shared_ptr<MockWriter> & writer,
        std::vector<Block> & source_blocks,
        bool empty_last_packet) const
    {
        dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
        prepareBlocks(source_blocks, empty_last_packet);

        const size_t batch_send_min_limit = 4096;
        auto dag_writer = std::make_shared<StreamingDAGResponseWriter<MockWriterPtr>>(
            writer,
            0,
            batch_send_min_limit,
            *dag_context_ptr);

        // 2. encode all blocks
        for (const auto & block : source_blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 3. send execution summary
        writer->add_summary = true;
        tipb::SelectResponse execution_summary_response;
        writer->write(execution_summary_response);
    }

    static void checkChunkInResponse(
        std::vector<Block> & source_blocks,
        std::vector<Block> & decoded_blocks,
        std::shared_ptr<MockExchangeReceiverInputStream> & receiver_stream,
        std::shared_ptr<MockWriter> & writer)
    {
        /// Check Connection Info
        auto infos = receiver_stream->getConnectionProfileInfos();
        ASSERT_EQ(infos.size(), 1);
        ASSERT_EQ(infos[0].packets, writer->total_packets);
        ASSERT_EQ(infos[0].bytes, writer->total_bytes);

        Block reference_block = squashBlocks(source_blocks);
        Block decoded_block = squashBlocks(decoded_blocks);
        ASSERT_EQ(receiver_stream->getTotalRows(), reference_block.rows());
        ASSERT_BLOCK_EQ(reference_block, decoded_block);
    }

    static void checkNoChunkInResponse(
        std::vector<Block> & source_blocks,
        std::vector<Block> & decoded_blocks,
        std::shared_ptr<MockExchangeReceiverInputStream> & receiver_stream,
        std::shared_ptr<MockWriter> & writer)
    {
        assert(receiver_stream);
        /// Check Execution Summary
        const auto & summary = receiver_stream->getRemoteExecutionSummary();
        ASSERT_EQ(summary.execution_summaries.size(), 1);
        ASSERT_EQ(summary.execution_summaries.begin()->first, "Executor_0");
        ASSERT_TRUE(equalSummaries(writer->mockExecutionSummary(), summary.execution_summaries.begin()->second));

        /// Check Connection Info
        auto infos = receiver_stream->getConnectionProfileInfos();
        ASSERT_EQ(infos.size(), 1);
        ASSERT_EQ(infos[0].packets, writer->total_packets);
        ASSERT_EQ(infos[0].bytes, writer->total_bytes);

        Block reference_block = squashBlocks(source_blocks);
        Block decoded_block = squashBlocks(decoded_blocks);
        ASSERT_BLOCK_EQ(reference_block, decoded_block);
        ASSERT_EQ(receiver_stream->getTotalRows(), reference_block.rows());
    }

    static std::shared_ptr<MockExchangeReceiverInputStream> makeExchangeReceiverInputStream(
        PacketQueuePtr queue_ptr,
        const ContextPtr & context)
    {
        auto receiver = std::make_shared<MockExchangeReceiver>(
            std::make_shared<MockReceiverContext>(queue_ptr, makeFields()),
            1,
            1,
            "mock_req_id",
            "mock_exchange_receiver_id",
            0,
            context->getSettingsRef());
        auto receiver_stream
            = std::make_shared<MockExchangeReceiverInputStream>(receiver, "mock_req_id", "executor_0", 0);
        return receiver_stream;
    }

    void doTestNoChunkInResponse(bool empty_last_packet) const
    {
        PacketQueuePtr queue_ptr = std::make_shared<PacketQueue>(1000);
        std::vector<Block> source_blocks;
        auto writer = std::make_shared<MockWriter>(*dag_context_ptr, queue_ptr);
        prepareQueue(writer, source_blocks, empty_last_packet);
        queue_ptr->finish();

        auto receiver_stream = makeExchangeReceiverInputStream(queue_ptr, context);
        receiver_stream->readPrefix();
        std::vector<Block> decoded_blocks;
        while (const auto & block = receiver_stream->read())
            decoded_blocks.emplace_back(block);
        receiver_stream->readSuffix();
        checkNoChunkInResponse(source_blocks, decoded_blocks, receiver_stream, writer);
    }

    void doTestChunkInResponse(bool empty_last_packet) const
    {
        PacketQueuePtr queue_ptr = std::make_shared<PacketQueue>(1000);
        std::vector<Block> source_blocks;
        auto writer = std::make_shared<MockWriter>(*dag_context_ptr, queue_ptr);
        prepareQueueV2(writer, source_blocks, empty_last_packet);
        queue_ptr->finish();
        auto receiver_stream = makeExchangeReceiverInputStream(queue_ptr, context);
        receiver_stream->readPrefix();
        std::vector<Block> decoded_blocks;
        while (const auto & block = receiver_stream->read())
            decoded_blocks.emplace_back(block);
        receiver_stream->readSuffix();
        checkChunkInResponse(source_blocks, decoded_blocks, receiver_stream, writer);
    }

    std::unique_ptr<DAGContext> dag_context_ptr{};
    ContextPtr context;
};

TEST_F(TestTiRemoteBlockInputStream, testNoChunkInResponse)
try
{
    doTestNoChunkInResponse(true);
    doTestNoChunkInResponse(false);
}
CATCH

TEST_F(TestTiRemoteBlockInputStream, testChunksInResponse)
try
{
    doTestChunkInResponse(true);
    doTestChunkInResponse(false);
}
CATCH

} // namespace tests
} // namespace DB

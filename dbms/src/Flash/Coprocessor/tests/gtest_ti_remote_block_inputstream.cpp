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

#include <DataStreams/SquashingTransform.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ExecutionSummaryCollector.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/Transaction/TiDB.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

#include <Flash/Coprocessor/ExecutionSummaryCollector.cpp>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.cpp>
#include <Flash/Mpp/ExchangeReceiver.cpp>
#include <memory>

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
        (left.scan_context->total_dmfile_scanned_rows == right.scan_context->total_dmfile_scanned_rows) && //
        (left.scan_context->total_dmfile_skipped_rows == right.scan_context->total_dmfile_skipped_rows);
}

struct MockWriter
{
    explicit MockWriter(PacketQueuePtr queue_)
        : queue(queue_)
    {}

    static ExecutionSummary mockExecutionSummary()
    {
        ExecutionSummary summary;
        summary.time_processed_ns = 100;
        summary.num_produced_rows = 10000;
        summary.num_iterations = 50;
        summary.concurrency = 1;
        summary.scan_context = std::make_unique<DM::ScanContext>();

        summary.scan_context->total_dmfile_scanned_packs = 1;
        summary.scan_context->total_dmfile_skipped_packs = 2;
        summary.scan_context->total_dmfile_scanned_rows = 8000;
        summary.scan_context->total_dmfile_skipped_rows = 15000;
        summary.scan_context->total_dmfile_rough_set_index_load_time_ms = 10;
        summary.scan_context->total_dmfile_read_time_ms = 200;
        summary.scan_context->total_create_snapshot_time_ms = 5;
        return summary;
    }

    void partitionWrite(TrackedMppDataPacketPtr &&, uint16_t) { FAIL() << "cannot reach here."; }
    void broadcastOrPassThroughWrite(TrackedMppDataPacketPtr && packet)
    {
        ++total_packets;
        if (!packet->packet.chunks().empty())
            total_bytes += packet->packet.ByteSizeLong();
        queue->push(std::move(packet));
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
    void sendExecutionSummary(const tipb::SelectResponse & response)
    {
        tipb::SelectResponse tmp = response;
        write(tmp);
    }
    uint16_t getPartitionNum() const { return 1; }

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
            if (queue->pop(res) == MPMCQueueResult::OK)
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

        void cancel(const String &)
        {
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
        PacketQueuePtr & queue_,
        const std::vector<tipb::FieldType> & field_types_)
        : queue(queue_)
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

    std::shared_ptr<Reader> makeReader(const Request &)
    {
        return std::make_shared<Reader>(queue);
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
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        dag_context_ptr->is_mpp_task = true;
        dag_context_ptr->is_root_mpp_task = true;
        dag_context_ptr->result_field_types = makeFields();
        dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
        context.setDAGContext(dag_context_ptr.get());
    }

public:
    TestTiRemoteBlockInputStream()
        : context(TiFlashTestEnv::getContext())
    {}

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
            block.insert(ColumnWithTypeAndName{
                std::move(int64_column),
                int64_data_type,
                String("col") + std::to_string(i)});
        }
        return block;
    }

    static void prepareBlocks(
        std::vector<Block> & source_blocks,
        bool empty_last_packet)
    {
        const size_t block_rows = 8192 * 3 / 8;
        /// 61 is specially chosen so that the last packet contains 3072 rows, and then end of the queue
        size_t block_num = empty_last_packet ? 60 : 61;
        // 1. Build Blocks.
        for (size_t i = 0; i < block_num; ++i)
            source_blocks.emplace_back(prepareBlock(block_rows));
    }

    void prepareQueue(
        std::shared_ptr<MockWriter> & writer,
        std::vector<Block> & source_blocks,
        bool empty_last_packet)
    {
        prepareBlocks(source_blocks, empty_last_packet);

        const size_t batch_send_min_limit = 4096;
        auto dag_writer = std::make_shared<BroadcastOrPassThroughWriter<MockWriterPtr>>(
            writer,
            batch_send_min_limit,
            *dag_context_ptr);

        // 2. encode all blocks
        for (const auto & block : source_blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 3. send execution summary
        writer->add_summary = true;
        ExecutionSummaryCollector summary_collector(*dag_context_ptr);
        writer->sendExecutionSummary(summary_collector.genExecutionSummaryResponse());
    }

    void prepareQueueV2(
        std::shared_ptr<MockWriter> & writer,
        std::vector<Block> & source_blocks,
        bool empty_last_packet)
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
        ExecutionSummaryCollector summary_collector(*dag_context_ptr);
        auto execution_summary_response = summary_collector.genExecutionSummaryResponse();
        writer->write(execution_summary_response);
    }

    void checkChunkInResponse(
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

    void checkNoChunkInResponse(
        std::vector<Block> & source_blocks,
        std::vector<Block> & decoded_blocks,
        std::shared_ptr<MockExchangeReceiverInputStream> & receiver_stream,
        std::shared_ptr<MockWriter> & writer)
    {
        assert(receiver_stream);
        /// Check Execution Summary
        const auto * summary = receiver_stream->getRemoteExecutionSummaries(0);
        ASSERT_TRUE(summary != nullptr);
        ASSERT_EQ(summary->size(), 1);
        ASSERT_EQ(summary->begin()->first, "Executor_0");
        ASSERT_TRUE(equalSummaries(writer->mockExecutionSummary(), summary->begin()->second));

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

    std::shared_ptr<MockExchangeReceiverInputStream> makeExchangeReceiverInputStream(
        PacketQueuePtr queue_ptr)
    {
        auto receiver = std::make_shared<MockExchangeReceiver>(
            std::make_shared<MockReceiverContext>(queue_ptr, makeFields()),
            1,
            1,
            "mock_req_id",
            "mock_exchange_receiver_id",
            0);
        auto receiver_stream = std::make_shared<MockExchangeReceiverInputStream>(
            receiver,
            "mock_req_id",
            "executor_0",
            0);
        return receiver_stream;
    }

    void doTestNoChunkInResponse(bool empty_last_packet)
    {
        PacketQueuePtr queue_ptr = std::make_shared<PacketQueue>(1000);
        std::vector<Block> source_blocks;
        auto writer = std::make_shared<MockWriter>(queue_ptr);
        prepareQueue(writer, source_blocks, empty_last_packet);
        queue_ptr->finish();

        auto receiver_stream = makeExchangeReceiverInputStream(queue_ptr);
        receiver_stream->readPrefix();
        std::vector<Block> decoded_blocks;
        while (const auto & block = receiver_stream->read())
            decoded_blocks.emplace_back(block);
        receiver_stream->readSuffix();
        checkNoChunkInResponse(source_blocks, decoded_blocks, receiver_stream, writer);
    }

    void doTestChunkInResponse(bool empty_last_packet)
    {
        PacketQueuePtr queue_ptr = std::make_shared<PacketQueue>(1000);
        std::vector<Block> source_blocks;
        auto writer = std::make_shared<MockWriter>(queue_ptr);
        prepareQueueV2(writer, source_blocks, empty_last_packet);
        queue_ptr->finish();
        auto receiver_stream = makeExchangeReceiverInputStream(queue_ptr);
        receiver_stream->readPrefix();
        std::vector<Block> decoded_blocks;
        while (const auto & block = receiver_stream->read())
            decoded_blocks.emplace_back(block);
        receiver_stream->readSuffix();
        checkChunkInResponse(source_blocks, decoded_blocks, receiver_stream, writer);
    }

    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr{};
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

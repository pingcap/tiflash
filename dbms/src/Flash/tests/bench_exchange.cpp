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

#include <Flash/tests/bench_exchange.h>
#include <fmt/core.h>

#include <Flash/Mpp/ExchangeReceiver.cpp> // to include the implementation of ExchangeReceiver
#include <Flash/Mpp/FineGrainedShuffleWriter.cpp> // to include the implementation of FineGrainedShuffleWriter
#include <Flash/Mpp/HashParitionWriter.cpp> // to include the implementation of HashParitionWriter
#include <Flash/Mpp/MPPTunnel.cpp> // to include the implementation of MPPTunnel
#include <Flash/Mpp/MPPTunnelSet.cpp> // to include the implementation of MPPTunnelSet
#include <atomic>
#include <chrono>


namespace DB
{
namespace tests
{

std::random_device rd;

MockBlockInputStream::MockBlockInputStream(const std::vector<Block> & blocks_, StopFlag & stop_flag_)
    : blocks(blocks_)
    , header(blocks[0].cloneEmpty())
    , mt(rd())
    , dist(0, blocks.size() - 1)
    , stop_flag(stop_flag_)
{}

MockFixedRowsBlockInputStream::MockFixedRowsBlockInputStream(size_t total_rows_, const std::vector<Block> & blocks_)
    : header(blocks_[0].cloneEmpty())
    , mt(rd())
    , dist(0, blocks_.size() - 1)
    , current_rows(0)
    , total_rows(total_rows_)
    , blocks(blocks_)
{}

Block makeBlock(int row_num, bool skew)
{
    InferredDataVector<Nullable<Int64>> int64_vec;
    InferredDataVector<Nullable<Int64>> int64_vec2;
    InferredDataVector<Nullable<String>> string_vec;

    if (skew)
    {
        for (int i = 0; i < row_num; ++i)
        {
            int64_vec.emplace_back(100);
            int64_vec2.emplace_back(100);
        }

        for (int i = 0; i < row_num; ++i)
        {
            string_vec.push_back("abcdefg");
        }
    }
    else
    {
        std::mt19937 mt(rd());
        std::uniform_int_distribution<Int64> int64_dist;
        std::uniform_int_distribution<int> len_dist(10, 20);
        std::uniform_int_distribution<std::int8_t> char_dist;

        for (int i = 0; i < row_num; ++i)
        {
            int64_vec.emplace_back(int64_dist(mt));
            int64_vec2.emplace_back(int64_dist(mt));
        }

        for (int i = 0; i < row_num; ++i)
        {
            int len = len_dist(mt);
            String s;
            for (int j = 0; j < len; ++j)
                s.push_back(char_dist(mt));
            string_vec.push_back(std::move(s));
        }
    }

    auto int64_data_type = makeDataType<Nullable<Int64>>();
    ColumnWithTypeAndName int64_column(makeColumn<Nullable<Int64>>(int64_data_type, int64_vec), int64_data_type, "int64_1");
    ColumnWithTypeAndName int64_column2(makeColumn<Nullable<Int64>>(int64_data_type, int64_vec2), int64_data_type, "int64_2");

    auto string_data_type = makeDataType<Nullable<String>>();
    ColumnWithTypeAndName string_column(makeColumn<Nullable<String>>(string_data_type, string_vec), string_data_type, "string");

    return Block({int64_column, string_column, int64_column2});
}

std::vector<Block> makeBlocks(int block_num, int row_num, bool skew)
{
    std::vector<Block> blocks;
    for (int i = 0; i < block_num; ++i)
        blocks.push_back(makeBlock(row_num, skew));
    return blocks;
}

mpp::MPPDataPacket makePacket(ChunkCodecStream & codec, int row_num)
{
    auto block = makeBlock(row_num);
    codec.encode(block, 0, row_num);

    mpp::MPPDataPacket packet;
    packet.add_chunks(codec.getString());
    codec.clear();

    return packet;
}

std::vector<PacketPtr> makePackets(ChunkCodecStream & codec, int packet_num, int row_num)
{
    std::vector<PacketPtr> packets;
    for (int i = 0; i < packet_num; ++i)
        packets.push_back(std::make_shared<Packet>(makePacket(codec, row_num)));
    return packets;
}

std::vector<PacketQueuePtr> makePacketQueues(int source_num, int queue_size)
{
    std::vector<PacketQueuePtr> queues(source_num);
    for (int i = 0; i < source_num; ++i)
        queues[i] = std::make_shared<PacketQueue>(queue_size);
    return queues;
}

std::vector<tipb::FieldType> makeFields()
{
    std::vector<tipb::FieldType> fields(3);
    fields[0].set_tp(TiDB::TypeLongLong);
    fields[1].set_tp(TiDB::TypeString);
    fields[2].set_tp(TiDB::TypeLongLong);
    return fields;
}

void printException(const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl
              << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl
                  << e.getStackTrace().toString() << std::endl;
}

ReceiverHelper::ReceiverHelper(int concurrency_, int source_num_, uint32_t fine_grained_shuffle_stream_count_)
    : concurrency(concurrency_)
    , source_num(source_num_)
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
{
    pb_exchange_receiver.set_tp(tipb::Hash);
    for (int i = 0; i < source_num; ++i)
    {
        mpp::TaskMeta task;
        task.set_start_ts(0);
        task.set_task_id(i);
        task.set_partition_id(i);
        task.set_address("");

        String encoded_task;
        task.SerializeToString(&encoded_task);

        pb_exchange_receiver.add_encoded_task_meta(encoded_task);
    }

    fields = makeFields();
    *pb_exchange_receiver.add_field_types() = fields[0];
    *pb_exchange_receiver.add_field_types() = fields[1];
    *pb_exchange_receiver.add_field_types() = fields[2];

    task_meta.set_task_id(100);

    queues = makePacketQueues(source_num, 10);
}

MockExchangeReceiverPtr ReceiverHelper::buildReceiver()
{
    return std::make_shared<MockExchangeReceiver>(
        std::make_shared<MockReceiverContext>(queues, fields),
        source_num,
        concurrency,
        "mock_req_id",
        "mock_exchange_receiver_id",
        fine_grained_shuffle_stream_count);
}

std::vector<BlockInputStreamPtr> ReceiverHelper::buildExchangeReceiverStream()
{
    auto receiver = buildReceiver();
    std::vector<BlockInputStreamPtr> streams(concurrency);
    // NOTE: check if need fine_grained_shuffle_stream_count
    for (int i = 0; i < concurrency; ++i)
    {
        streams[i] = std::make_shared<MockExchangeReceiverInputStream>(receiver,
                                                                       "mock_req_id",
                                                                       "mock_executor_id" + std::to_string(i),
                                                                       /*stream_id=*/enableFineGrainedShuffle(fine_grained_shuffle_stream_count) ? i : 0);
    }
    return streams;
}

BlockInputStreamPtr ReceiverHelper::buildUnionStream()
{
    auto streams = buildExchangeReceiverStream();
    return std::make_shared<UnionBlockInputStream<>>(streams, BlockInputStreams{}, concurrency, /*req_id=*/"");
}

void ReceiverHelper::finish()
{
    if (join_ptr)
    {
        join_ptr->setBuildTableState(Join::BuildTableState::SUCCEED);
        std::cout << fmt::format("Hash table size: {} bytes", join_ptr->getTotalByteCount()) << std::endl;
    }
}

SenderHelper::SenderHelper(
    int source_num_,
    int concurrency_,
    uint32_t fine_grained_shuffle_stream_count_,
    int64_t fine_grained_shuffle_batch_size_,
    const std::vector<PacketQueuePtr> & queues_,
    const std::vector<tipb::FieldType> & fields)
    : source_num(source_num_)
    , concurrency(concurrency_)
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
    , queues(queues_)
{
    mpp::TaskMeta task_meta;
    tunnel_set = std::make_shared<MockTunnelSet>("mock_req_id");
    for (int i = 0; i < source_num; ++i)
    {
        auto writer = std::make_shared<MockWriter>(queues[i]);
        mock_writers.push_back(writer);

        auto tunnel = std::make_shared<MockTunnel>(
            task_meta,
            task_meta,
            std::chrono::seconds(60),
            concurrency,
            false,
            false,
            "mock_req_id");
        tunnel->connect(writer.get());
        tunnels.push_back(tunnel);
        MPPTaskId id(0, i);
        tunnel_set->registerTunnel(id, tunnel);
    }

    tipb::DAGRequest dag_request;
    tipb::Executor root_executor;
    root_executor.set_executor_id("ExchangeSender_100");
    *dag_request.mutable_root_executor() = root_executor;

    dag_context = std::make_unique<DAGContext>(dag_request);
    dag_context->is_mpp_task = true;
    dag_context->is_root_mpp_task = false;
    dag_context->encode_type = tipb::EncodeType::TypeCHBlock;
    dag_context->result_field_types = fields;
}

BlockInputStreamPtr SenderHelper::buildUnionStream(
    StopFlag & stop_flag,
    const std::vector<Block> & blocks)
{
    std::vector<BlockInputStreamPtr> send_streams;
    for (int i = 0; i < concurrency; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<MockBlockInputStream>(blocks, stop_flag);
        if (enableFineGrainedShuffle(fine_grained_shuffle_stream_count))
        {
            std::unique_ptr<DAGResponseWriter> response_writer(
                new FineGrainedShuffleWriter<MockTunnelSetPtr>(
                    tunnel_set,
                    {0, 1, 2},
                    TiDB::TiDBCollators(3),
                    true,
                    *dag_context,
                    fine_grained_shuffle_stream_count,
                    fine_grained_shuffle_batch_size));
            send_streams.push_back(std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), /*req_id=*/""));
        }
        else
        {
            std::unique_ptr<DAGResponseWriter> response_writer(
                new HashParitionWriter<MockTunnelSetPtr>(
                    tunnel_set,
                    {0, 1, 2},
                    TiDB::TiDBCollators(3),
                    -1,
                    true,
                    *dag_context));
            send_streams.push_back(std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), /*req_id=*/""));
        }
    }

    return std::make_shared<UnionBlockInputStream<>>(send_streams, BlockInputStreams{}, concurrency, /*req_id=*/"");
}

BlockInputStreamPtr SenderHelper::buildUnionStream(size_t total_rows, const std::vector<Block> & blocks)
{
    std::vector<BlockInputStreamPtr> send_streams;
    for (int i = 0; i < concurrency; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<MockFixedRowsBlockInputStream>(total_rows / concurrency, blocks);
        if (enableFineGrainedShuffle(fine_grained_shuffle_stream_count))
        {
            std::unique_ptr<DAGResponseWriter> response_writer(
                new FineGrainedShuffleWriter<MockTunnelSetPtr>(
                    tunnel_set,
                    {0, 1, 2},
                    TiDB::TiDBCollators(3),
                    true,
                    *dag_context,
                    fine_grained_shuffle_stream_count,
                    fine_grained_shuffle_batch_size));
            send_streams.push_back(std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), /*req_id=*/""));
        }
        else
        {
            std::unique_ptr<DAGResponseWriter> response_writer(
                new HashParitionWriter<MockTunnelSetPtr>(
                    tunnel_set,
                    {0, 1, 2},
                    TiDB::TiDBCollators(3),
                    -1,
                    true,
                    *dag_context));
            send_streams.push_back(std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), /*req_id=*/""));
        }
    }

    return std::make_shared<UnionBlockInputStream<>>(send_streams, BlockInputStreams{}, concurrency, /*req_id=*/"");
}

void SenderHelper::finish()
{
    for (size_t i = 0; i < tunnels.size(); ++i)
    {
        tunnels[i]->writeDone();
        tunnels[i]->waitForFinish();
        mock_writers[i]->finish();
    }
}

void ExchangeBench::SetUp(const benchmark::State &)
{
    DynamicThreadPool::global_instance = std::make_unique<DynamicThreadPool>(
        /*fixed_thread_num=*/300,
        std::chrono::milliseconds(100000));

    uniform_blocks = makeBlocks(/*block_num=*/100, /*row_num=*/1024);
    skew_blocks = makeBlocks(/*block_num=*/100, /*row_num=*/1024, /*skew=*/true);

    try
    {
        DB::registerWindowFunctions();
        DB::registerFunctions();
    }
    catch (DB::Exception &)
    {
        // Maybe another test has already registered, ignore exception here.
    }
}

void ExchangeBench::TearDown(const benchmark::State &)
{
    uniform_blocks.clear();
    skew_blocks.clear();
    // NOTE: Must reset here, otherwise DynamicThreadPool::fixedWork() may core because metrics already destroyed.
    DynamicThreadPool::global_instance.reset();
}

void ExchangeBench::runAndWait(std::shared_ptr<ReceiverHelper> receiver_helper,
                               BlockInputStreamPtr receiver_stream,
                               std::shared_ptr<SenderHelper> & sender_helper,
                               BlockInputStreamPtr sender_stream)
{
    std::future<void> sender_future = DynamicThreadPool::global_instance->schedule(/*memory_tracker=*/false,
                                                                                   [sender_stream, sender_helper] {
                                                                                       sender_stream->readPrefix();
                                                                                       while (const auto & block = sender_stream->read()) {}
                                                                                       sender_stream->readSuffix();
                                                                                       sender_helper->finish();
                                                                                   });
    std::future<void> receiver_future = DynamicThreadPool::global_instance->schedule(/*memory_tracker=*/false,
                                                                                     [receiver_stream, receiver_helper] {
                                                                                         receiver_stream->readPrefix();
                                                                                         while (const auto & block = receiver_stream->read()) {}
                                                                                         receiver_stream->readSuffix();
                                                                                         receiver_helper->finish();
                                                                                     });
    sender_future.get();
    receiver_future.get();
}

BENCHMARK_DEFINE_F(ExchangeBench, basic_send_receive)
(benchmark::State & state)
try
{
    const int concurrency = state.range(0);
    const int source_num = state.range(1);
    const int total_rows = state.range(2);
    const int fine_grained_shuffle_stream_count = state.range(3);
    const int fine_grained_shuffle_batch_size = state.range(4);
    Context context = TiFlashTestEnv::getContext();

    for (auto _ : state)
    {
        std::shared_ptr<ReceiverHelper> receiver_helper = std::make_shared<ReceiverHelper>(concurrency, source_num, fine_grained_shuffle_stream_count);
        BlockInputStreamPtr receiver_stream = receiver_helper->buildUnionStream();

        std::shared_ptr<SenderHelper> sender_helper = std::make_shared<SenderHelper>(source_num,
                                                                                     concurrency,
                                                                                     fine_grained_shuffle_stream_count,
                                                                                     fine_grained_shuffle_batch_size,
                                                                                     receiver_helper->queues,
                                                                                     receiver_helper->fields);
        BlockInputStreamPtr sender_stream = sender_helper->buildUnionStream(total_rows, uniform_blocks);

        runAndWait(receiver_helper, receiver_stream, sender_helper, sender_stream);
    }
}
CATCH
BENCHMARK_REGISTER_F(ExchangeBench, basic_send_receive)
    ->Args({8, 1, 1024 * 1000, 0, 4096})
    ->Args({8, 1, 1024 * 1000, 4, 4096})
    ->Args({8, 1, 1024 * 1000, 8, 4096})
    ->Args({8, 1, 1024 * 1000, 16, 4096})
    ->Args({8, 1, 1024 * 1000, 32, 4096})
    ->Args({8, 1, 1024 * 1000, 8, 1})
    ->Args({8, 1, 1024 * 1000, 8, 1000})
    ->Args({8, 1, 1024 * 1000, 8, 10000})
    ->Args({8, 1, 1024 * 1000, 8, 100000});


} // namespace tests
} // namespace DB

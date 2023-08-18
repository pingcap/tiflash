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

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/MPMCQueue.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Interpreters/Join.h>
#include <TestUtils/FunctionTestUtils.h>
#include <fmt/core.h>

#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp> // to include the implementation of StreamingDAGResponseWriter
#include <Flash/Mpp/ExchangeReceiver.cpp> // to include the implementation of ExchangeReceiver
#include <Flash/Mpp/MPPTunnel.cpp> // to include the implementation of MPPTunnel
#include <Flash/Mpp/MPPTunnelSet.cpp> // to include the implementation of MPPTunnelSet
#include <atomic>
#include <chrono>
#include <random>

namespace DB::tests
{
namespace
{
std::random_device rd;

using Packet = mpp::MPPDataPacket;
using PacketPtr = std::shared_ptr<Packet>;
using PacketQueue = MPMCQueue<PacketPtr>;
using PacketQueuePtr = std::shared_ptr<PacketQueue>;
using StopFlag = std::atomic<bool>;

std::atomic<Int64> received_data_size{0};

struct MockReceiverContext
{
    struct Status
    {
        int status_code = 0;
        String error_msg;

        bool ok() const
        {
            return status_code == 0;
        }

        const String & error_message() const
        {
            return error_msg;
        }

        int error_code() const
        {
            return status_code;
        }
    };

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
                received_data_size.fetch_add(res->ByteSizeLong());
                *packet = *res; // avoid change shared packets
                return true;
            }
            return false;
        }

        Status finish() const
        {
            return {0, ""};
        }

        PacketQueuePtr queue;
    };

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
        return {0, ""};
    }

    std::vector<PacketQueuePtr> queues;
    std::vector<tipb::FieldType> field_types;
};

using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;
using MockExchangeReceiverPtr = std::shared_ptr<MockExchangeReceiver>;
using MockExchangeReceiverInputStream = TiRemoteBlockInputStream<MockExchangeReceiver>;

struct MockWriter
{
    explicit MockWriter(PacketQueuePtr queue_)
        : queue(std::move(queue_))
    {}

    bool Write(const Packet & packet)
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

    MockBlockInputStream(const std::vector<Block> & blocks_, StopFlag & stop_flag_)
        : blocks(blocks_)
        , header(blocks[0].cloneEmpty())
        , mt(rd())
        , dist(0, blocks.size() - 1)
        , stop_flag(stop_flag_)
    {}

    String getName() const override { return "MockBlockInputStream"; }
    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (stop_flag.load(std::memory_order_relaxed))
            return Block{};
        return blocks[dist(mt)];
    }
};

Block makeBlock(int row_num)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<Int64> int64_dist;
    std::uniform_int_distribution<int> len_dist(10, 20);
    std::uniform_int_distribution<char> char_dist;

    InferredDataVector<Nullable<Int64>> int64_vec;
    InferredDataVector<Nullable<Int64>> int64_vec2;
    for (int i = 0; i < row_num; ++i)
    {
        int64_vec.emplace_back(int64_dist(mt));
        int64_vec2.emplace_back(int64_dist(mt));
    }

    InferredDataVector<Nullable<String>> string_vec;
    for (int i = 0; i < row_num; ++i)
    {
        int len = len_dist(mt);
        String s;
        for (int j = 0; j < len; ++j)
            s.push_back(char_dist(mt));
        string_vec.push_back(std::move(s));
    }

    auto int64_data_type = makeDataType<Nullable<Int64>>();
    ColumnWithTypeAndName int64_column(makeColumn<Nullable<Int64>>(int64_data_type, int64_vec), int64_data_type, "int64_1");
    ColumnWithTypeAndName int64_column2(makeColumn<Nullable<Int64>>(int64_data_type, int64_vec2), int64_data_type, "int64_2");

    auto string_data_type = makeDataType<Nullable<String>>();
    ColumnWithTypeAndName string_column(makeColumn<Nullable<String>>(string_data_type, string_vec), string_data_type, "string");

    return Block({int64_column, string_column, int64_column2});
}

std::vector<Block> makeBlocks(int block_num, int row_num)
{
    std::vector<Block> blocks;
    for (int i = 0; i < block_num; ++i)
        blocks.push_back(makeBlock(row_num));
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
    std::vector<PacketQueuePtr> queues;
    for (int i = 0; i < source_num; ++i)
        queues.push_back(std::make_shared<PacketQueue>(queue_size));
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

void sendPacket(const std::vector<PacketPtr> & packets, const PacketQueuePtr & queue, StopFlag & stop_flag)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, packets.size() - 1);

    while (!stop_flag.load())
    {
        int i = dist(mt);
        queue->tryPush(packets[i], std::chrono::milliseconds(10));
    }
    queue->finish();
}

void receivePacket(const PacketQueuePtr & queue)
{
    while (true)
    {
        PacketPtr packet;
        if (queue->pop(packet))
            received_data_size.fetch_add(packet->ByteSizeLong());
        else
            break;
    }
}

template <bool print_progress>
void readBlock(BlockInputStreamPtr stream)
{
    [[maybe_unused]] auto get_rate = [](auto count, auto duration) {
        return count * 1000 / duration.count();
    };

    [[maybe_unused]] auto get_mib = [](auto v) {
        return v / 1024 / 1024;
    };

    [[maybe_unused]] auto start = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] auto second_ago = start;
    [[maybe_unused]] Int64 block_count = 0;
    [[maybe_unused]] Int64 last_block_count = 0;
    [[maybe_unused]] Int64 last_data_size = received_data_size.load();
    try
    {
        stream->readPrefix();
        while (auto block = stream->read())
        {
            if constexpr (print_progress)
            {
                ++block_count;
                auto cur = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - second_ago);
                if (duration.count() >= 1000)
                {
                    Int64 data_size = received_data_size.load();
                    std::cout
                        << fmt::format(
                               "Blocks: {:<10} Data(MiB): {:<8} Block/s: {:<6} Data/s(MiB): {:<6}",
                               block_count,
                               get_mib(data_size),
                               get_rate(block_count - last_block_count, duration),
                               get_mib(get_rate(data_size - last_data_size, duration)))
                        << std::endl;
                    second_ago = cur;
                    last_block_count = block_count;
                    last_data_size = data_size;
                }
            }
        }
        stream->readSuffix();

        if constexpr (print_progress)
        {
            auto cur = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - start);
            Int64 data_size = received_data_size.load();
            std::cout
                << fmt::format(
                       "End. Blocks: {:<10} Data(MiB): {:<8} Block/s: {:<6} Data/s(MiB): {:<6}",
                       block_count,
                       get_mib(data_size),
                       get_rate(block_count, duration),
                       get_mib(get_rate(data_size, duration)))
                << std::endl;
        }
    }
    catch (const Exception & e)
    {
        printException(e);
        throw;
    }
}

struct ReceiverHelper
{
    const int source_num;
    tipb::ExchangeReceiver pb_exchange_receiver;
    std::vector<tipb::FieldType> fields;
    mpp::TaskMeta task_meta;
    std::vector<PacketQueuePtr> queues;
    std::shared_ptr<Join> join_ptr;

    explicit ReceiverHelper(int source_num_)
        : source_num(source_num_)
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

    MockExchangeReceiverPtr buildReceiver()
    {
        return std::make_shared<MockExchangeReceiver>(
            std::make_shared<MockReceiverContext>(queues, fields),
            source_num,
            source_num * 5,
            nullptr);
    }

    BlockInputStreamPtr buildUnionStream(int concurrency)
    {
        auto receiver = buildReceiver();
        std::vector<BlockInputStreamPtr> streams;
        for (int i = 0; i < concurrency; ++i)
            streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(receiver, nullptr));
        return std::make_shared<UnionBlockInputStream<>>(streams, nullptr, concurrency, /*req_id=*/"");
    }

    BlockInputStreamPtr buildUnionStreamWithHashJoinBuildStream(int concurrency)
    {
        auto receiver = buildReceiver();
        std::vector<BlockInputStreamPtr> streams;
        for (int i = 0; i < concurrency; ++i)
            streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(receiver, nullptr));

        auto receiver_header = streams.front()->getHeader();
        auto key_name = receiver_header.getByPosition(0).name;

        join_ptr = std::make_shared<Join>(
            Names{key_name},
            Names{key_name},
            true,
            SizeLimits(0, 0, OverflowMode::THROW),
            ASTTableJoin::Kind::Inner,
            ASTTableJoin::Strictness::All,
            concurrency,
            TiDB::TiDBCollators{nullptr},
            "",
            "",
            "",
            "",
            nullptr,
            65536);

        join_ptr->setSampleBlock(receiver_header);

        for (int i = 0; i < concurrency; ++i)
            streams[i] = std::make_shared<HashJoinBuildBlockInputStream>(streams[i], join_ptr, i, /*req_id=*/"");

        return std::make_shared<UnionBlockInputStream<>>(streams, nullptr, concurrency, /*req_id=*/"");
    }

    void finish()
    {
        if (join_ptr)
        {
            join_ptr->setBuildTableState(Join::BuildTableState::SUCCEED);
            std::cout << fmt::format("Hash table size: {} bytes", join_ptr->getTotalByteCount()) << std::endl;
        }
    }
};

struct SenderHelper
{
    const int source_num;
    const int concurrency;

    std::vector<PacketQueuePtr> queues;
    std::vector<MockWriterPtr> mock_writers;
    std::vector<MockTunnelPtr> tunnels;
    MockTunnelSetPtr tunnel_set;
    std::unique_ptr<DAGContext> dag_context;

    SenderHelper(
        int source_num_,
        int concurrency_,
        const std::vector<PacketQueuePtr> & queues_,
        const std::vector<tipb::FieldType> & fields)
        : source_num(source_num_)
        , concurrency(concurrency_)
        , queues(queues_)
    {
        mpp::TaskMeta task_meta;
        tunnel_set = std::make_shared<MockTunnelSet>();
        for (int i = 0; i < source_num; ++i)
        {
            auto writer = std::make_shared<MockWriter>(queues[i]);
            mock_writers.push_back(writer);

            auto tunnel = std::make_shared<MockTunnel>(
                task_meta,
                task_meta,
                std::chrono::seconds(60),
                concurrency,
                false);
            tunnel->connect(writer.get());
            tunnels.push_back(tunnel);
            tunnel_set->addTunnel(tunnel);
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

    BlockInputStreamPtr buildUnionStream(
        StopFlag & stop_flag,
        const std::vector<Block> & blocks)
    {
        std::vector<BlockInputStreamPtr> send_streams;
        for (int i = 0; i < concurrency; ++i)
        {
            BlockInputStreamPtr stream = std::make_shared<MockBlockInputStream>(blocks, stop_flag);
            std::unique_ptr<DAGResponseWriter> response_writer(
                new StreamingDAGResponseWriter<MockTunnelSetPtr>(
                    tunnel_set,
                    {0, 1, 2},
                    TiDB::TiDBCollators(3),
                    tipb::Hash,
                    -1,
                    -1,
                    true,
                    *dag_context));
            send_streams.push_back(std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), /*req_id=*/""));
        }

        return std::make_shared<UnionBlockInputStream<>>(send_streams, nullptr, concurrency, /*req_id=*/"");
    }

    void finish()
    {
        for (size_t i = 0; i < tunnels.size(); ++i)
        {
            tunnels[i]->writeDone();
            tunnels[i]->waitForFinish();
            mock_writers[i]->finish();
        }
    }
};

void testOnlyReceiver(int concurrency, int source_num, int block_rows, int seconds)
{
    ReceiverHelper receiver_helper(source_num);
    auto union_input_stream = receiver_helper.buildUnionStream(concurrency);

    auto chunk_codec_stream = CHBlockChunkCodec().newCodecStream(receiver_helper.fields);
    auto packets = makePackets(*chunk_codec_stream, 100, block_rows);

    StopFlag stop_flag(false);

    std::vector<std::thread> threads;
    for (const auto & queue : receiver_helper.queues)
        threads.emplace_back(sendPacket, std::cref(packets), queue, std::ref(stop_flag));
    threads.emplace_back(readBlock<true>, union_input_stream);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);
    for (auto & thread : threads)
        thread.join();

    receiver_helper.finish();
}

template <bool with_join>
void testSenderReceiver(int concurrency, int source_num, int block_rows, int seconds)
{
    ReceiverHelper receiver_helper(source_num);
    BlockInputStreamPtr union_receive_stream;
    if constexpr (with_join)
        union_receive_stream = receiver_helper.buildUnionStreamWithHashJoinBuildStream(concurrency);
    else
        union_receive_stream = receiver_helper.buildUnionStream(concurrency);

    StopFlag stop_flag(false);
    auto blocks = makeBlocks(100, block_rows);

    SenderHelper sender_helper(source_num, concurrency, receiver_helper.queues, receiver_helper.fields);
    auto union_send_stream = sender_helper.buildUnionStream(stop_flag, blocks);

    auto write_thread = std::thread(readBlock<false>, union_send_stream);
    auto read_thread = std::thread(readBlock<true>, union_receive_stream);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);

    write_thread.join();
    sender_helper.finish();

    read_thread.join();
    receiver_helper.finish();
}

void testOnlySender(int concurrency, int source_num, int block_rows, int seconds)
{
    auto queues = makePacketQueues(source_num, 10);
    auto fields = makeFields();

    StopFlag stop_flag(false);
    auto blocks = makeBlocks(100, block_rows);

    SenderHelper sender_helper(source_num, concurrency, queues, fields);
    auto union_send_stream = sender_helper.buildUnionStream(stop_flag, blocks);

    auto write_thread = std::thread(readBlock<true>, union_send_stream);
    std::vector<std::thread> read_threads;
    for (int i = 0; i < source_num; ++i)
        read_threads.emplace_back(receivePacket, queues[i]);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);

    write_thread.join();
    sender_helper.finish();

    for (auto & t : read_threads)
        t.join();
}

} // namespace
} // namespace DB::tests

int main(int argc [[maybe_unused]], char ** argv [[maybe_unused]])
{
    if (argc < 2 || argc > 6)
    {
        std::cerr << fmt::format("Usage: {} [receiver|sender|sender_receiver|sender_receiver_join] <concurrency=5> <source_num=2> <block_rows=5000> <seconds=10>", argv[0]) << std::endl;
        exit(1);
    }

    String method = argv[1];
    int concurrency = argc >= 3 ? atoi(argv[2]) : 5;
    int source_num = argc >= 4 ? atoi(argv[3]) : 2;
    int block_rows = argc >= 5 ? atoi(argv[4]) : 5000;
    int seconds = argc >= 6 ? atoi(argv[5]) : 10;

    using TestHandler = std::function<void(int concurrency, int source_num, int block_rows, int seconds)>;
    std::unordered_map<String, TestHandler> handlers = {
        {"receiver", DB::tests::testOnlyReceiver},
        {"sender", DB::tests::testOnlySender},
        {"sender_receiver", DB::tests::testSenderReceiver<false>},
        {"sender_receiver_join", DB::tests::testSenderReceiver<true>},
    };

    auto it = handlers.find(method);
    if (it != handlers.end())
    {
        std::cout
            << fmt::format(
                   "{}. concurrency = {}. source_num = {}. block_rows = {}. seconds = {}",
                   method,
                   concurrency,
                   source_num,
                   block_rows,
                   seconds)
            << std::endl;
        it->second(concurrency, source_num, block_rows, seconds);
    }
    else
    {
        std::cerr << "Unknown method: " << method << std::endl;
        exit(1);
    }
}

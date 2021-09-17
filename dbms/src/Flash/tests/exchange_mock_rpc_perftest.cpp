#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Join.h>
#include <TestUtils/FunctionTestUtils.h>
#include <fmt/core.h>

#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp>
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
    };

    struct Request
    {
        String DebugString() const
        {
            return "{Request}";
        }

        int send_task_id = 0;
    };

    struct Reader
    {
        explicit Reader(const PacketQueuePtr & queue_)
            : queue(queue_)
        {}

        void initialize() const
        {
        }

        bool read(mpp::MPPDataPacket * packet [[maybe_unused]]) const
        {
            auto res = queue->pop();
            if (!res.has_value() || !res.value())
                return false;
            *packet = *res.value();
            received_data_size.fetch_add(packet->ByteSizeLong());
            return true;
        }

        Status finish() const
        {
            return {0, ""};
        }

        PacketQueuePtr queue;
    };

    explicit MockReceiverContext(const std::vector<PacketQueuePtr> & queues_)
        : queues(queues_)
    {
    }

    Request makeRequest(
        int index [[maybe_unused]],
        const tipb::ExchangeReceiver & pb_exchange_receiver [[maybe_unused]],
        const ::mpp::TaskMeta & task_meta [[maybe_unused]]) const
    {
        return {index};
    }

    std::shared_ptr<Reader> makeReader(const Request & request [[maybe_unused]])
    {
        return std::make_shared<Reader>(queues[request.send_task_id]);
    }

    static Status getStatusOK()
    {
        return {0, ""};
    }

    std::vector<PacketQueuePtr> queues;
};

using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;

struct MockWriter
{
    explicit MockWriter(const std::vector<PacketQueuePtr> & queues_)
        : queues(queues_)
    {}

    void write(tipb::SelectResponse & response)
    {
        auto packet = std::make_shared<Packet>();
        response.SerializeToString(packet->mutable_data());
        for (const auto & queue : queues)
            queue->push(std::move(packet));
    }

    void write(tipb::SelectResponse & response, int16_t i)
    {
        auto packet = std::make_shared<Packet>();
        response.SerializeToString(packet->mutable_data());
        queues[i]->push(std::move(packet));
    }

    void finish()
    {
        for (const auto & queue : queues)
            queue->finish();
    }

    uint16_t getPartitionNum() const
    {
        return queues.size();
    }

    std::vector<PacketQueuePtr> queues;
};

struct MockBlockInputStream : public IProfilingBlockInputStream
{
    const std::vector<Block> & blocks;
    StopFlag & stop_flag;
    Block header;
    std::mt19937 mt;
    std::uniform_int_distribution<int> dist;

    MockBlockInputStream(const std::vector<Block> & blocks_, StopFlag & stop_flag_)
        : blocks(blocks_)
        , stop_flag(stop_flag_)
        , header(blocks[0].cloneEmpty())
        , mt(rd())
        , dist(0, blocks.size() - 1)
    {}

    String getName() const override { return "MockBlockInputStream"; }
    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (!stop_flag.load(std::memory_order_acquire))
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

mpp::MPPDataPacket makePacket(ChunkCodecStream & codec, int row_num)
{
    auto block = makeBlock(row_num);
    codec.encode(block, 0, row_num);

    tipb::SelectResponse response;
    response.set_encode_type(tipb::TypeCHBlock);
    auto chunk = response.add_chunks();
    chunk->set_rows_data(codec.getString());
    codec.clear();

    mpp::MPPDataPacket packet;
    response.SerializeToString(packet.mutable_data());
    return packet;
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

using BlockWriter = StreamingDAGResponseWriter<std::shared_ptr<MockWriter>>;
using BlockWriterPtr = std::shared_ptr<BlockWriter>;

void sendBlock(
    const std::vector<Block> & blocks,
    const BlockOutputStreamPtr & out,
    const std::shared_ptr<MockWriter> & raw_writer [[maybe_unused]],
    const StopFlag & stop_flag)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, blocks.size() - 1);

    out->writePrefix();
    while (!stop_flag.load())
    {
        int i = dist(mt);
        out->write(blocks[i]);
    }
    out->writeSuffix();
}

void readBlock(BlockInputStreamPtr stream)
{
    auto get_rate = [](auto count, auto duration) {
        return count * 1000 / duration.count();
    };

    auto get_mib = [](auto v) {
        return v / 1024 / 1024;
    };

    auto start = std::chrono::high_resolution_clock::now();
    auto second_ago = start;
    Int64 block_count = 0;
    Int64 last_block_count = 0;
    Int64 last_data_size = received_data_size.load();
    try
    {
        stream->readPrefix();
        while (auto block = stream->read())
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
        stream->readSuffix();

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
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        auto embedded_stack_trace_pos = text.find("Stack trace");
        std::cerr << "Code: " << e.code() << ". " << text << std::endl
                  << std::endl;
        if (std::string::npos == embedded_stack_trace_pos)
            std::cerr << "Stack trace:" << std::endl
                      << e.getStackTrace().toString() << std::endl;
        exit(1);
    }
}

void testOnlyReceiver(int concurrency, int source_num, int block_rows, int seconds)
{
    std::cout
        << fmt::format(
               "receiver. concurrency = {}. source_num = {}. block_rows = {}. seconds = {}",
               concurrency,
               source_num,
               block_rows,
               seconds)
        << std::endl;

    tipb::ExchangeReceiver pb_exchange_receiver;
    pb_exchange_receiver.set_tp(tipb::PassThrough);
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

    tipb::FieldType f0, f1, f2;

    f0.set_tp(TiDB::TypeLongLong);
    f1.set_tp(TiDB::TypeString);
    f2.set_tp(TiDB::TypeLongLong);

    *pb_exchange_receiver.add_field_types() = f0;
    *pb_exchange_receiver.add_field_types() = f1;
    *pb_exchange_receiver.add_field_types() = f2;

    mpp::TaskMeta task_meta;
    task_meta.set_task_id(100);

    auto chunk_codec_stream = CHBlockChunkCodec().newCodecStream({f0, f1, f2});
    std::vector<PacketPtr> packets;
    for (int i = 0; i < 100; ++i)
        packets.push_back(std::make_shared<Packet>(makePacket(*chunk_codec_stream, block_rows)));

    std::vector<PacketQueuePtr> queues;
    for (int i = 0; i < source_num; ++i)
        queues.push_back(std::make_shared<PacketQueue>(10));

    auto context = std::make_shared<MockReceiverContext>(queues);
    StopFlag stop_flag(false);

    auto receiver = std::make_shared<MockExchangeReceiver>(
        context,
        pb_exchange_receiver,
        task_meta,
        source_num * 5);

    using MockExchangeReceiverInputStream = TiRemoteBlockInputStream<MockExchangeReceiver>;
    std::vector<BlockInputStreamPtr> streams;
    for (int i = 0; i < concurrency; ++i)
        streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(receiver));
    auto union_input_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, source_num);

    std::vector<std::thread> threads;
    for (const auto & queue : queues)
        threads.emplace_back(sendPacket, std::cref(packets), queue, std::ref(stop_flag));
    threads.emplace_back(readBlock, union_input_stream);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);
    for (auto & thread : threads)
        thread.join();
}

template <bool with_join>
void testSenderReceiver(int concurrency, int source_num, int block_rows, int seconds)
{
    std::cout
        << fmt::format(
               "sender_receiver. concurrency = {}. source_num = {}. block_rows = {}. seconds = {}",
               concurrency,
               source_num,
               block_rows,
               seconds)
        << std::endl;

    tipb::ExchangeReceiver pb_exchange_receiver;
    pb_exchange_receiver.set_tp(tipb::PassThrough);
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

    tipb::FieldType f0, f1, f2;

    f0.set_tp(TiDB::TypeLongLong);
    f1.set_tp(TiDB::TypeString);
    f2.set_tp(TiDB::TypeLongLong);

    *pb_exchange_receiver.add_field_types() = f0;
    *pb_exchange_receiver.add_field_types() = f1;
    *pb_exchange_receiver.add_field_types() = f2;

    mpp::TaskMeta task_meta;
    task_meta.set_task_id(100);

    std::vector<Block> blocks;
    for (int i = 0; i < 100; ++i)
        blocks.push_back(makeBlock(block_rows));

    std::vector<PacketQueuePtr> queues;
    for (int i = 0; i < source_num; ++i)
        queues.push_back(std::make_shared<PacketQueue>(10));

    auto context = std::make_shared<MockReceiverContext>(queues);
    StopFlag stop_flag(false);

    auto receiver = std::make_shared<MockExchangeReceiver>(
        context,
        pb_exchange_receiver,
        task_meta,
        source_num * 5);

    using MockExchangeReceiverInputStream = TiRemoteBlockInputStream<MockExchangeReceiver>;
    std::vector<BlockInputStreamPtr> streams;
    for (int i = 0; i < concurrency; ++i)
        streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(receiver));

    auto receiver_header = streams.front()->getHeader();

    std::shared_ptr<Join> join_ptr;
    if constexpr (with_join)
    {
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

        for (int i = 0; i < concurrency; ++i)
            streams[i] = std::make_shared<HashJoinBuildBlockInputStream>(streams[i], join_ptr, i);
    }

    auto union_input_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, concurrency);

    auto mock_writer = std::make_shared<MockWriter>(queues);

    DAGContext dag_context(tipb::DAGRequest{});
    dag_context.final_concurrency = concurrency;
    dag_context.is_mpp_task = true;
    dag_context.is_root_mpp_task = false;
    std::unique_ptr<DAGResponseWriter> response_writer(
        new BlockWriter(
            mock_writer,
            {0, 1, 2},
            TiDB::TiDBCollators(3),
            tipb::Hash,
            -1,
            tipb::TypeCHBlock,
            {f0, f1, f2},
            dag_context));

    BlockOutputStreamPtr output_stream = std::make_shared<DAGBlockOutputStream>(union_input_stream->getHeader(), std::move(response_writer));
    output_stream = std::make_shared<SquashingBlockOutputStream>(output_stream, 20000, 0);

    if constexpr (with_join)
    {
        if (join_ptr)
            join_ptr->setSampleBlock(receiver_header);
    }

    auto write_thread = std::thread(sendBlock, std::cref(blocks), output_stream, mock_writer, std::ref(stop_flag));
    auto read_thread = std::thread(readBlock, union_input_stream);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);

    write_thread.join();
    mock_writer->finish();
    read_thread.join();

    if constexpr (with_join)
    {
        if (join_ptr)
        {
            join_ptr->setFinishBuildTable(true);
            std::cout << fmt::format("Hash table size: {} bytes", join_ptr->getTotalByteCount()) << std::endl;
        }
    }
}

} // namespace
} // namespace DB::tests

int main(int argc [[maybe_unused]], char ** argv [[maybe_unused]])
{
    if (argc < 2 || argc > 6)
    {
        std::cerr << fmt::format("Usage: {} [receiver|sender_receiver] <concurrency=5> <source_num=2> <block_rows=5000> <seconds=10>", argv[0]) << std::endl;
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
        {"sender_receiver", DB::tests::testSenderReceiver<false>},
        {"sender_receiver_join", DB::tests::testSenderReceiver<true>},
    };

    auto it = handlers.find(method);
    if (it != handlers.end())
        it->second(concurrency, source_num, block_rows, seconds);
    else
    {
        std::cerr << "Unknown method: " << method << std::endl;
        exit(1);
    }
}

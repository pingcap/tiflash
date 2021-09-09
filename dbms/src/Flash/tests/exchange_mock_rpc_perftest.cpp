#include <Common/ConcurrentBoundedQueue.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <TestUtils/FunctionTestUtils.h>
#include <atomic>
#include <chrono>
#include <fmt/core.h>
#include <random>

namespace DB::tests
{
namespace
{

std::random_device rd;

using PacketPtr = std::shared_ptr<mpp::MPPDataPacket>;
using PacketQueue = ConcurrentBoundedQueue<PacketPtr>;

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
        Reader(PacketQueue & queue_, std::atomic<Int64> & counter)
            : queue(queue_), size_counter(counter)
        {}

        void initialize() const
        {
        }

        bool read(mpp::MPPDataPacket * packet [[maybe_unused]]) const
        {
            PacketPtr ptr;
            queue.pop(ptr);
            if (!ptr)
                return false;
            *packet = std::move(*ptr);
            size_counter.fetch_add(packet->ByteSizeLong());
            return true;
        }

        Status finish() const
        {
            return {0, ""};
        }

        PacketQueue & queue;
        std::atomic<Int64> & size_counter;
    };

    explicit MockReceiverContext(PacketQueue & queue_)
        : queue(queue_)
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
        return std::make_shared<Reader>(queue, received_data_size);
    }

    static Status getStatusOK()
    {
        return {0, ""};
    }

    PacketQueue & queue;
};
    
using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;

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
    auto chunk = response.add_chunks();
    chunk->set_rows_data(codec.getString());
    codec.clear();

    mpp::MPPDataPacket packet;
    response.SerializeToString(packet.mutable_data());
    return packet;
}

void sendPacket(const std::vector<PacketPtr> & packets, int source_num, PacketQueue & queue, std::atomic<bool> & stop_flag)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, packets.size() - 1);

    while (!stop_flag.load())
    {
        int i = dist(mt);
        queue.tryPush(packets[i], 10);
    }
    for (int i = 0; i < source_num; ++i)
        queue.push(PacketPtr());
}

template <typename ExchangeReceiverType>
void receivePacket(ExchangeReceiverType & receiver)
{
    auto start = std::chrono::high_resolution_clock::now();
    auto second_ago = start;
    Int64 packet_count = 0;
    Int64 last_data_size = received_data_size.load();
    while (true)
    {
        auto res = receiver.nextResult();
        if (res.eof)
            break;
        ++packet_count;
        auto cur = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - second_ago);
        if (duration.count() >= 1000)
        {
            Int64 data_size = received_data_size.load();
            std::cout
                << fmt::format(
                    "Packets: {:<12} Data: {:<12} Rate(MiB): {:<6}",
                    packet_count,
                    data_size,
                    (data_size - last_data_size) * 1000 / duration.count() / 1024 / 1024)
                << std::endl;
            second_ago = cur;
            last_data_size = data_size;
        }
    }

    auto cur = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - start);
    Int64 data_size = received_data_size.load();
    std::cout
        << fmt::format(
            "Packets: {:<12} Data: {:<12} Rate(MiB): {:<6}. End",
            packet_count,
            data_size,
            data_size * 1000 / duration.count() / 1024 / 1024)
        << std::endl;
}

void testReceiver(int source_num, int block_rows, int seconds)
{
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
        packets.push_back(std::make_shared<mpp::MPPDataPacket>(makePacket(*chunk_codec_stream, block_rows)));
    
    PacketQueue queue(source_num * 5);
    auto context = std::make_shared<MockReceiverContext>(queue);
    std::atomic<bool> stop_flag(false);

    ExchangeReceiverBase<MockReceiverContext> receiver(
        context,
        pb_exchange_receiver,
        task_meta,
        source_num * 5);

    std::thread send_thread([&] { sendPacket(packets, source_num, queue, stop_flag); });
    std::thread receive_thread([&] { receivePacket(receiver); });

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop_flag.store(true);
    send_thread.join();
    receive_thread.join();

    packets.clear();
}

} // namespace
} // namespace DB::tests

int main(int argc [[maybe_unused]], char ** argv [[maybe_unused]])
{
    if (argc != 4)
    {
        std::cerr << fmt::format("Usage: {} <source_num> <block_rows> <seconds>", argv[0]) << std::endl;
        exit(1);
    }

    int source_num = atoi(argv[1]);
    int block_rows = atoi(argv[2]);
    int seconds = atoi(argv[3]);

    if (source_num <= 0)
        source_num = 5;
    if (block_rows <= 0)
        block_rows = 1000;
    if (seconds <= 0)
        seconds = 30;

    DB::tests::testReceiver(source_num, block_rows, seconds);
}


#include <Common/ConcurrentBoundedQueue.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <TestUtils/FunctionTestUtils.h>
#include <fmt/core.h>
#include <random>

namespace DB::tests
{
namespace
{

std::random_device rd;

struct MockReceiverContext
{
    using PacketQueue = std::queue<mpp::MPPDataPacket>;

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
        explicit Reader(PacketQueue & queue)
            : packets(queue)
        {}

        void initialize() const
        {
        }

        bool read(mpp::MPPDataPacket * packet [[maybe_unused]]) const
        {
            if (packets.empty())
                return false;
            *packet = std::move(packets.front());
            packets.pop();
            return true;
        }

        Status finish() const
        {
            return {0, ""};
        }

        PacketQueue & packets;
    };

    explicit MockReceiverContext(int source_num)
    {
        packets_vec.resize(source_num);
    }

    Request makeRequest(
        int index [[maybe_unused]],
        const tipb::ExchangeReceiver & pb_exchange_receiver [[maybe_unused]],
        const ::mpp::TaskMeta & task_meta [[maybe_unused]]) const
    {
        return {index};
    }

    std::shared_ptr<Reader> makeReader(const Request & request)
    {
        return std::make_shared<Reader>(packets_vec[request.send_task_id]);
    }

    static Status getStatusOK()
    {
        return {0, ""};
    }

    std::vector<PacketQueue> packets_vec;
};

Block makeBlock(int row_num)
{
    std::mt19937 mt(rd());
    std::uniform_int_distribution<Int64> int64_dist;
    std::uniform_int_distribution<int> len_dist(10, 20);
    std::uniform_int_distribution<char> char_dist;

    InferredDataVector<Nullable<Int64>> int64_vec;
    for (int i = 0; i < row_num; ++i)
    {
        int64_vec.emplace_back(int64_dist(mt));
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
    ColumnWithTypeAndName int64_column(makeColumn<Nullable<Int64>>(int64_data_type, int64_vec), int64_data_type, "int64");

    auto string_data_type = makeDataType<Nullable<String>>();
    ColumnWithTypeAndName string_column(makeColumn<Nullable<String>>(string_data_type, string_vec), string_data_type, "string");

    return Block({int64_column, string_column});
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

void testReceiver(int source_num)
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

    tipb::FieldType f0, f1;

    f0.set_tp(TiDB::TypeLongLong);
    f1.set_tp(TiDB::TypeString);

    *pb_exchange_receiver.add_field_types() = f0;
    *pb_exchange_receiver.add_field_types() = f1;

    mpp::TaskMeta task_meta;
    task_meta.set_task_id(100);

    auto chunk_codec_stream = CHBlockChunkCodec().newCodecStream({f0, f1});

    auto context = std::make_shared<MockReceiverContext>(source_num);
    for (int i = 0; i < source_num; ++i)
        for (int j = 0; j < 100; ++j)
            context->packets_vec[i].push(makePacket(*chunk_codec_stream, 100));

    ExchangeReceiverBase<MockReceiverContext> receiver(
        context,
        pb_exchange_receiver,
        task_meta,
        source_num * 5);

    while (true)
    {
        auto res = receiver.nextResult();
        std::cout
            << fmt::format(
                    "res ci={} info={} meet_error={} error_msg={} eof={} chunk={} size={}",
                    res.call_index,
                    res.req_info,
                    res.meet_error,
                    res.error_msg,
                    res.eof,
                    res.eof ? -1 : res.resp->chunks_size(),
                    res.eof ? -1 : res.resp->chunks(0).rows_data().size())
            << std::endl;
        if (res.eof)
            break;
    }
}

} // namespace
} // namespace DB::tests

int main(int argc [[maybe_unused]], char ** argv [[maybe_unused]])
{
    DB::tests::testReceiver(5);
}


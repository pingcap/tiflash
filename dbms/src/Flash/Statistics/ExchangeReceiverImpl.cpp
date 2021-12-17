#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/ExchangeReceiverImpl.h>

namespace DB
{
String ExchangeReceiveDetail::toJson() const
{
    return fmt::format(
        R"({{"receiver_source_task_id":{},"packets":{},"bytes":{}}})",
        receiver_source_task_id,
        packets,
        bytes);
}

void ExchangeReceiverStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("partition_num":{},"receiver_source_task_ids":[{}],"connection_details":[)",
        partition_num,
        fmt::join(receiver_source_task_ids, ","));
    fmt_buffer.joinStr(
        exchange_receive_details.cbegin(),
        exchange_receive_details.cend(),
        [](const auto & p, FmtBuffer & bf) { bf.append(p.toJson()); },
        ",");
    fmt_buffer.append("]");
}

void ExchangeReceiverStatistics::collectExtraRuntimeDetail()
{
    const auto & io_stream_map = dag_context.getInBoundIOInputStreamsMap();
    auto it = io_stream_map.find(executor_id);
    if (it != io_stream_map.end())
    {
        for (const auto & io_stream : it->second)
        {
            auto * exchange_receiver_stream = dynamic_cast<ExchangeReceiverInputStream *>(io_stream.get());
            /// InBoundIOInputStream of ExchangeReceiver should be ExchangeReceiverInputStream
            assert(exchange_receiver_stream);
            const auto & connection_profile_infos = exchange_receiver_stream->getConnectionProfileInfos();
            assert(connection_profile_infos.size() == partition_num);
            for (size_t i = 0; i < partition_num; ++i)
            {
                exchange_receive_details[i].packets += connection_profile_infos[i].packets;
                exchange_receive_details[i].bytes += connection_profile_infos[i].bytes;
            }
        }
    }
}

ExchangeReceiverStatistics::ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExchangeReceiverStatisticsBase(executor, dag_context_)
{
    assert(executor->tp() == tipb::ExecType::TypeExchangeReceiver);
    const auto & exchange_sender_receiver = executor->exchange_receiver();
    partition_num = exchange_sender_receiver.encoded_task_meta_size();

    for (size_t index = 0; index < partition_num; ++index)
    {
        auto sender_task = mpp::TaskMeta{};
        if (!sender_task.ParseFromString(exchange_sender_receiver.encoded_task_meta(index)))
            throw Exception("parse task meta error!");
        receiver_source_task_ids.push_back(sender_task.task_id());

        ExchangeReceiveDetail detail;
        detail.receiver_source_task_id = sender_task.task_id();
        exchange_receive_details.push_back(std::move(detail));
    }
}
} // namespace DB
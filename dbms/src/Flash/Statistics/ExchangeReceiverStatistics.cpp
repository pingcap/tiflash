#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiveDetail::toJson() const
{
    return fmt::format(
        R"({{receiver_source_task_id":{},"packets":{},"bytes":{}}})",
        receiver_source_task_id,
        connection_profile_info.packets,
        connection_profile_info.bytes);
}

String ExchangeReceiverStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"partition_num":{},"receiver_source_task_ids":[{}],"connection_details":{})",
        partition_num,
        fmt::join(receiver_source_task_ids, ","),
        arrayToJson(exchange_receive_details));
}

bool ExchangeReceiverStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeReceiver_");
}

ExchangeReceiverStatistics::ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExecutorStatistics(executor, dag_context_)
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

void ExchangeReceiverStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeReceiver should not be zero"),
            Errors::Coprocessor::Internal);

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeReceiverInputStream>(stream_ptr, [&](const ExchangeReceiverInputStream & stream) {
                    collectBaseInfo(this, stream.getProfileInfo());

                    const auto & stream_profile_infos = stream.getConnectionProfileInfos();
                    assert(stream_profile_infos.size() == partition_num);
                    for (size_t i = 0; i < partition_num; ++i)
                    {
                        auto & exchange_profile_info = exchange_receive_details[i].connection_profile_info;
                        exchange_profile_info.packets += stream_profile_infos[i].packets;
                        exchange_profile_info.bytes += stream_profile_infos[i].bytes;
                    }
                }),
                stream_ptr->getName(),
                "ExchangeReceiverInputStream");
        });
}
} // namespace DB
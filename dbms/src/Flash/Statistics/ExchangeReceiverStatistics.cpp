#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiverStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"partition_num":{},"receiver_source_task_ids":[{}])",
        partition_num,
        fmt::join(receiver_source_task_ids, ","));
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
                }),
                stream_ptr->getName(),
                "ExchangeReceiverInputStream");
        });
}
} // namespace DB
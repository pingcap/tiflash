#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/ExchangeReceiveProfileInfo.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiverStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"partition_num":{},"receiver_source_task_ids":[{}],"connection_details":{})",
        partition_num,
        fmt::join(receiver_source_task_ids, ","),
        arrayToJson(connection_profile_infos));
}

bool ExchangeReceiverStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeReceiver_");
}

ExchangeReceiverStatistics::ExchangeReceiverStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
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
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeReceiver should not be zero"),
            Errors::Coprocessor::Internal);

    bool need_merge = profile_streams_info.input_streams.size() > 1;

    const auto & last_stream_ptr = profile_streams_info.input_streams.back();
    throwFailCastException(
        castBlockInputStream<ExchangeReceiverInputStream>(
            last_stream_ptr,
            [&](const ExchangeReceiverInputStream & stream) {
                if (need_merge)
                {
                    connection_profile_infos = stream.getRemoteReader()->createConnectionProfileInfos();
                }
                else
                {
                    connection_profile_infos = stream.getConnectionProfileInfos();
                }
                assert(partition_num == connection_profile_infos.size());
            }),
        last_stream_ptr->getName(),
        "ExchangeReceiverInputStream");

    if (need_merge)
    {
        visitBlockInputStreams(
            profile_streams_info.input_streams,
            [&](const BlockInputStreamPtr & stream_ptr) {
                throwFailCastException(
                    castBlockInputStream<ExchangeReceiverInputStream>(stream_ptr, [&](const ExchangeReceiverInputStream & stream) {
                        collectBaseInfo(this, stream.getProfileInfo());

                        const auto & er_profile_infos = stream.getConnectionProfileInfos();
                        assert(er_profile_infos.size() == partition_num);
                        for (size_t i = 0; i < partition_num; ++i)
                        {
                            const auto & connection_profile_info = connection_profile_infos[i];
                            connection_profile_info->packets += er_profile_infos[i]->packets;
                            connection_profile_info->bytes += er_profile_infos[i]->bytes;
                        }
                    }),
                    stream_ptr->getName(),
                    "ExchangeReceiverInputStream");
            });
    }
}
} // namespace DB
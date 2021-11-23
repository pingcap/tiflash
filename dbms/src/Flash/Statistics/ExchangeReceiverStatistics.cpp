#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExchangeReceiveProfileInfo.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiverStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","partition_num":{},"downstream_task_ids":[{}],"connection_details":{}}})",
        id,
        type,
        partition_num,
        fmt::join(downstream_task_ids, ","),
        arrayToJson(connection_profile_infos));
}

bool ExchangeReceiverStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeReceiver_");
}

ExecutorStatisticsPtr ExchangeReceiverStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    using ExchangeReceiverStatisticsPtr = std::shared_ptr<ExchangeReceiverStatistics>;
    ExchangeReceiverStatisticsPtr statistics = std::make_shared<ExchangeReceiverStatistics>(executor_id);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeReceiver should not be zero"),
            Errors::Coprocessor::Internal);

    castBlockInputStream<ExchangeReceiverInputStream>(
        profile_streams_info.input_streams.back(),
        [&](const ExchangeReceiverInputStream & stream) {
            statistics->partition_num = stream.getSourceNum();
            statistics->connection_profile_infos = stream.getRemoteReader()->createConnectionProfileInfos();
            assert(statistics->partition_num == statistics->connection_profile_infos.size());
            for (const ConnectionProfileInfoPtr & profile_info : statistics->connection_profile_infos)
            {
                if (auto * er = dynamic_cast<ExchangeReceiveProfileInfo *>(profile_info.get()))
                    statistics->downstream_task_ids.push_back(er->sender_task_id);
            }
            assert(statistics->partition_num == statistics->downstream_task_ids.size());
        });
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeReceiverInputStream>(stream_ptr, [&](const ExchangeReceiverInputStream & stream) {
                    const auto & er_profile_infos = stream.getConnectionProfileInfos();
                    assert(er_profile_infos.size() == statistics->partition_num);
                    for (size_t i = 0; i != statistics->partition_num; ++i)
                    {
                        const auto & connection_profile_info = statistics->connection_profile_infos[i];
                        const auto & inc_connection_profile_info = er_profile_infos[i];
                        connection_profile_info->rows += inc_connection_profile_info->rows;
                        connection_profile_info->blocks += inc_connection_profile_info->blocks;
                        connection_profile_info->bytes += inc_connection_profile_info->bytes;
                    }
                }),
                stream_ptr->getName(),
                "ExchangeReceiverInputStream");
        });

    return statistics;
}
} // namespace DB
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

ExecutorStatisticsPtr ExchangeReceiverStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using ExchangeReceiverStatisticsPtr = std::shared_ptr<ExchangeReceiverStatistics>;
    ExchangeReceiverStatisticsPtr statistics = std::make_shared<ExchangeReceiverStatistics>(executor_id, context);
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
                statistics->partition_num = stream.getSourceNum();
                if (need_merge)
                {
                    statistics->connection_profile_infos = stream.getRemoteReader()->createConnectionProfileInfos();
                }
                else
                {
                    statistics->connection_profile_infos = stream.getConnectionProfileInfos();
                }
                assert(statistics->partition_num == statistics->connection_profile_infos.size());
            }),
        last_stream_ptr->getName(),
        "ExchangeReceiverInputStream");

    for (const ConnectionProfileInfoPtr & profile_info : statistics->connection_profile_infos)
    {
        auto * er = dynamic_cast<ExchangeReceiveProfileInfo *>(profile_info.get());
        assert(er != nullptr);
        statistics->receiver_source_task_ids.push_back(er->receiver_source_task_id);
    }
    assert(statistics->partition_num == statistics->receiver_source_task_ids.size());

    if (need_merge)
    {
        visitBlockInputStreams(
            profile_streams_info.input_streams,
            [&](const BlockInputStreamPtr & stream_ptr) {
                throwFailCastException(
                    castBlockInputStream<ExchangeReceiverInputStream>(stream_ptr, [&](const ExchangeReceiverInputStream & stream) {
                        collectBaseInfo(statistics, stream.getProfileInfo());

                        const auto & er_profile_infos = stream.getConnectionProfileInfos();
                        assert(er_profile_infos.size() == statistics->partition_num);
                        for (size_t i = 0; i < statistics->partition_num; ++i)
                        {
                            const auto & connection_profile_info = statistics->connection_profile_infos[i];
                            connection_profile_info->packets += er_profile_infos[i]->packets;
                            connection_profile_info->bytes += er_profile_infos[i]->bytes;
                        }
                    }),
                    stream_ptr->getName(),
                    "ExchangeReceiverInputStream");
            });
    }

    return statistics;
}
} // namespace DB
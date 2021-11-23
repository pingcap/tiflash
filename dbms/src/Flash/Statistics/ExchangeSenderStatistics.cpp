#include <Common/TiFlashException.h>
#include <DataStreams/ExchangeSender.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Statistics/ExchangeSenderStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
namespace
{
String exchangeTypeToString(const tipb::ExchangeType & exchange_type)
{
    switch (exchange_type)
    {
    case tipb::ExchangeType::PassThrough:
        return "PassThrough";
    case tipb::ExchangeType::Broadcast:
        return "Broadcast";
    case tipb::ExchangeType::Hash:
        return "Hash";
    default:
        throw TiFlashException("unknown ExchangeType", Errors::Coprocessor::Internal);
    }
}
} // namespace

String ExchangeSenderStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","partition_num":{},"connection_details":{},"exchange_type":"{}"}})",
        id,
        type,
        partition_num,
        arrayToJson(connection_profile_infos),
        exchangeTypeToString(exchange_type));
}

bool ExchangeSenderStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeSender_");
}

ExecutorStatisticsPtr ExchangeSenderStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context)
{
    assert(dag_context.is_mpp_task);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeSender should not be zero"),
            Errors::Coprocessor::Internal);

    using ExchangeSenderStatisticsPtr = std::shared_ptr<ExchangeSenderStatistics>;
    ExchangeSenderStatisticsPtr statistics = std::make_shared<ExchangeSenderStatistics>(executor_id);

    const auto & tunnel_set = dag_context.tunnel_set;
    statistics->partition_num = tunnel_set->getPartitionNum();
    statistics->connection_profile_infos = tunnel_set->getConnectionProfileInfos();

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeSender>(stream_ptr, [&](const ExchangeSender &) {}),
                stream_ptr->getName(),
                "ExchangeSender");
        });
    castBlockInputStream<ExchangeSender>(
        profile_streams_info.input_streams.back(),
        [&](const ExchangeSender & exchange_sender) {
            statistics->exchange_type = exchange_sender.pb_exchange_sender.tp();
        });

    return statistics;
}
} // namespace DB
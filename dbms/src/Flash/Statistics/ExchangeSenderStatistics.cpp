#include <Common/TiFlashException.h>
#include <DataStreams/ExchangeSender.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Statistics/ExchangeSenderStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Interpreters/Context.h>
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

String ExchangeSenderStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"partition_num":{},"sender_target_task_ids":[{}],"connection_details":{},"exchange_type":"{}")",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        arrayToJson(connection_profile_infos),
        exchangeTypeToString(exchange_type));
}

bool ExchangeSenderStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeSender_");
}

ExecutorStatisticsPtr ExchangeSenderStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    auto & dag_context = *context.getDAGContext();
    assert(dag_context.is_mpp_task);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeSender should not be zero"),
            Errors::Coprocessor::Internal);

    using ExchangeSenderStatisticsPtr = std::shared_ptr<ExchangeSenderStatistics>;
    ExchangeSenderStatisticsPtr statistics = std::make_shared<ExchangeSenderStatistics>(executor_id, context);

    const auto & tunnel_set = dag_context.tunnel_set;
    statistics->partition_num = tunnel_set->getPartitionNum();
    statistics->connection_profile_infos = tunnel_set->getConnectionProfileInfos();

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeSender>(stream_ptr, [&](const ExchangeSender & stream) {
                    collectBaseInfo(statistics, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "ExchangeSender");
        });

    const auto * executor = context.getDAGContext()->getExecutor(executor_id);
    assert(executor->tp() == tipb::ExecType::TypeExchangeSender);
    const auto & exchange_sender_executor = executor->exchange_sender();
    assert(exchange_sender_executor.has_tp());
    statistics->exchange_type = exchange_sender_executor.tp();

    for (int i = 0; i < exchange_sender_executor.encoded_task_meta_size(); ++i)
    {
        mpp::TaskMeta task_meta;
        if (!task_meta.ParseFromString(exchange_sender_executor.encoded_task_meta(i)))
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        statistics->sender_target_task_ids.push_back(task_meta.task_id());
    }

    return statistics;
}
} // namespace DB
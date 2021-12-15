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

String MPPTunnelDetail::toJson() const
{
    return fmt::format(
        R"({{"tunnel_id":"{}","sender_target_task_id":{},"is_local":{},"packets":{},"bytes":{}}})",
        tunnel_id,
        sender_target_task_id,
        is_local,
        connection_profile_info.packets,
        connection_profile_info.bytes);
}

String ExchangeSenderStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"partition_num":{},"sender_target_task_ids":[{}],"connection_details":{},"exchange_type":"{}")",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        arrayToJson(mpp_tunnel_details),
        exchangeTypeToString(exchange_type));
}

ExchangeSenderStatistics::ExchangeSenderStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExecutorStatistics(executor, dag_context_)
{
    assert(dag_context.is_mpp_task);

    assert(executor->tp() == tipb::ExecType::TypeExchangeSender);
    const auto & exchange_sender_executor = executor->exchange_sender();
    assert(exchange_sender_executor.has_tp());
    exchange_type = exchange_sender_executor.tp();
    partition_num = exchange_sender_executor.encoded_task_meta_size();

    assert(dag_context.tunnel_set != nullptr);
    assert(partition_num == dag_context.tunnel_set->getPartitionNum());
    const auto & mpp_tunnels = dag_context.tunnel_set->getTunnels();
    for (int i = 0; i < exchange_sender_executor.encoded_task_meta_size(); ++i)
    {
        mpp::TaskMeta task_meta;
        if (!task_meta.ParseFromString(exchange_sender_executor.encoded_task_meta(i)))
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        sender_target_task_ids.push_back(task_meta.task_id());

        MPPTunnelDetail detail;
        detail.tunnel_id = mpp_tunnels[i]->id();
        detail.sender_target_task_id = task_meta.task_id();
        detail.is_local = mpp_tunnels[i]->isLocal();
        mpp_tunnel_details.push_back(std::move(detail));
    }
}

bool ExchangeSenderStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeSender_");
}

void ExchangeSenderStatistics::collectRuntimeDetail()
{
    assert(dag_context.is_mpp_task);

    const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeSender should not be zero"),
            Errors::Coprocessor::Internal);

    const auto & tunnel_set = dag_context.tunnel_set;
    assert(partition_num == tunnel_set->getPartitionNum());
    size_t i = 0;
    for (const auto & tunnel : tunnel_set->getTunnels())
    {
        mpp_tunnel_details[i++].connection_profile_info = tunnel->getConnectionProfileInfo();
    }

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeSender>(stream_ptr, [&](const ExchangeSender & stream) {
                    collectBaseInfo(this, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "ExchangeSender");
        });
}
} // namespace DB
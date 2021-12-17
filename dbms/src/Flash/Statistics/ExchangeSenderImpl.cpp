#include <Common/TiFlashException.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>

namespace DB
{
String MPPTunnelDetail::toJson() const
{
    return fmt::format(
        R"({{"tunnel_id":"{}","sender_target_task_id":{},"is_local":{},"packets":{},"bytes":{}}})",
        tunnel_id,
        sender_target_task_id,
        is_local,
        packets,
        bytes);
}

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

void ExchangeSenderStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("partition_num":{},"sender_target_task_ids":[{}],"exchange_type":"{}","connection_details":[)",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        exchangeTypeToString(exchange_type));
    fmt_buffer.joinStr(
        mpp_tunnel_details.cbegin(),
        mpp_tunnel_details.cend(),
        [](const auto & p, FmtBuffer & bf) { bf.append(p.toJson()); },
        ",");
    fmt_buffer.append("]");
}

ExchangeSenderStatistics::ExchangeSenderStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExchangeSenderStatisticsBase(executor, dag_context_)
{
    assert(dag_context.is_mpp_task);

    assert(executor->tp() == tipb::ExecType::TypeExchangeSender);
    const auto & exchange_sender_executor = executor->exchange_sender();
    assert(exchange_sender_executor.has_tp());
    exchange_type = exchange_sender_executor.tp();
    partition_num = exchange_sender_executor.encoded_task_meta_size();

    const auto & mpp_tunnel_set = dag_context.tunnel_set;
    assert(partition_num == mpp_tunnel_set->getPartitionNum());
    const auto & mpp_tunnels = mpp_tunnel_set->getTunnels();

    for (int i = 0; i < exchange_sender_executor.encoded_task_meta_size(); ++i)
    {
        mpp::TaskMeta task_meta;
        if (!task_meta.ParseFromString(exchange_sender_executor.encoded_task_meta(i)))
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        sender_target_task_ids.push_back(task_meta.task_id());

        const auto & mpp_tunnel = mpp_tunnels[i];
        MPPTunnelDetail detail;
        detail.tunnel_id = mpp_tunnel->id();
        detail.sender_target_task_id = task_meta.task_id();
        detail.is_local = mpp_tunnel->isLocal();
        mpp_tunnel_details.push_back(std::move(detail));
    }
}
} // namespace DB
#include <Common/TiFlashException.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>

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

void ExchangeSenderStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"(,"partition_num":{},"sender_target_task_ids":[{}],"exchange_type":"{}")",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        exchangeTypeToString(exchange_type));
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

    for (int i = 0; i < exchange_sender_executor.encoded_task_meta_size(); ++i)
    {
        mpp::TaskMeta task_meta;
        if (!task_meta.ParseFromString(exchange_sender_executor.encoded_task_meta(i)))
            throw TiFlashException("Failed to decode task meta info in ExchangeSender", Errors::Coprocessor::BadRequest);
        sender_target_task_ids.push_back(task_meta.task_id());
    }
}
} // namespace DB
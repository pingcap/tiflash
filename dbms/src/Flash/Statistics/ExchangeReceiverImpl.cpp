#include <Flash/Statistics/ExchangeReceiverImpl.h>

namespace DB
{
void ExchangeReceiverStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("partition_num":{},"receiver_source_task_ids":[{}])",
        partition_num,
        fmt::join(receiver_source_task_ids, ","));
}

ExchangeReceiverStatistics::ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExchangeReceiverStatisticsBase(executor, dag_context_)
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
} // namespace DB
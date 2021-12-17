#include <Flash/Statistics/ExchangeReceiverImpl.h>

namespace DB
{
String ExchangeReceiveDetail::toJson() const
{
    return fmt::format(
        R"({{receiver_source_task_id":{},"packets":{},"bytes":{}}})",
        receiver_source_task_id,
        packets,
        bytes);
}

void ExchangeReceiverStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("partition_num":{},"receiver_source_task_ids":[{}],"connection_details":[)",
        partition_num,
        fmt::join(receiver_source_task_ids, ","));
    fmt_buffer.joinStr(
        exchange_receive_details.cbegin(),
        exchange_receive_details.cend(),
        [](const auto & p, FmtBuffer & bf) { bf.append(p.toJson()); },
        ",");
    fmt_buffer.append("]");
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

        ExchangeReceiveDetail detail;
        detail.receiver_source_task_id = sender_task.task_id();
        exchange_receive_details.push_back(std::move(detail));
    }
}
} // namespace DB
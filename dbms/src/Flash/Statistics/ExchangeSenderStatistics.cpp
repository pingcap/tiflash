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
        R"(,"partition_num":{},"sender_target_task_ids":[{}],"exchange_type":"{}")",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        exchangeTypeToString(exchange_type));
}

ExchangeSenderStatistics::ExchangeSenderStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{
    auto & dag_context = *context.getDAGContext();
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

bool ExchangeSenderStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeSender_");
}

void ExchangeSenderStatistics::collectRuntimeDetail()
{
    auto & dag_context = *context.getDAGContext();
    assert(dag_context.is_mpp_task);

    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.empty())
        throw TiFlashException(
            fmt::format("Count of ExchangeSender should not be zero"),
            Errors::Coprocessor::Internal);

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
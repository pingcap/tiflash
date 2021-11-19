#include <DataStreams/ExchangeSender.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExchangeSenderStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeSenderStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}"}})",
        id,
        type);
}

bool ExchangeSenderStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeSender_");
}

ExecutorStatisticsPtr ExchangeSenderStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context)
{
    assert(dag_context.is_mpp_task);
    using ExchangeSenderStatisticsPtr = std::shared_ptr<ExchangeSenderStatistics>;
    ExchangeSenderStatisticsPtr statistics = std::make_shared<ExchangeSenderStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<ExchangeSender>(stream_ptr, [&](const ExchangeSender &) {}),
                stream_ptr->getName(),
                "ExchangeSender");
        });
    return statistics;
}
} // namespace DB
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExchangeSenderStatistics.h>
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

ExecutorStatisticsPtr ExchangeSenderStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    using ExchangeSenderStatisticsPtr = std::shared_ptr<ExchangeSenderStatistics>;
    ExchangeSenderStatisticsPtr statistics = std::make_shared<ExchangeSenderStatistics>(executor_id);
    return statistics;
}
} // namespace DB
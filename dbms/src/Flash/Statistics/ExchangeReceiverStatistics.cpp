#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiverStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}"}})",
        id,
        type);
}

bool ExchangeReceiverStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "ExchangeReceiver_");
}

ExecutorStatisticsPtr ExchangeReceiverStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    using ExchangeReceiverStatisticsPtr = std::shared_ptr<ExchangeReceiverStatistics>;
    ExchangeReceiverStatisticsPtr statistics = std::make_shared<ExchangeReceiverStatistics>(executor_id);
    return statistics;
}
} // namespace DB
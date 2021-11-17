#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String TableScanStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}"}})",
        id,
        type);
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
}

ExecutorStatisticsPtr TableScanStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    using TableScanStatisticsPtr = std::shared_ptr<TableScanStatistics>;
    TableScanStatisticsPtr statistics = std::make_shared<TableScanStatistics>(executor_id);
    return statistics;
}
} // namespace DB
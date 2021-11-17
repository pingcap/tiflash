#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/AggStatistics.h>
#include <Flash/Statistics/ExchangeReceiverStatistics.h>
#include <Flash/Statistics/ExchangeSenderStatistics.h>
#include <Flash/Statistics/FilterStatistics.h>
#include <Flash/Statistics/JoinStatistics.h>
#include <Flash/Statistics/LimitStatistics.h>
#include <Flash/Statistics/ProjectionStatistics.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <Flash/Statistics/TopNStatistics.h>
#include <Flash/Statistics/collectExecutorStatistics.h>

namespace DB
{
namespace
{
struct ExecutorStatisticsCollector
{
    DAGContext & dag_context;

    std::vector<ExecutorStatisticsPtr> res;

    explicit ExecutorStatisticsCollector(DAGContext & dag_context_)
        : dag_context(dag_context_)
    {}

    template <typename T>
    bool append(const String & executor_id, const ProfileStreamsInfo & profile_streams_info)
    {
        if (T::hit(executor_id))
        {
            res.push_back(T::buildStatistics(executor_id, profile_streams_info, dag_context));
            return true;
        }
        else
        {
            return false;
        }
    }
};
} // namespace

std::vector<ExecutorStatisticsPtr> collectExecutorStatistics(DAGContext & dag_context)
{
    ExecutorStatisticsCollector collector{dag_context};
    for (const auto & profile_streams_info_entry : dag_context.getProfileStreamsMap())
    {
        const auto & executor_id = profile_streams_info_entry.first;
        const auto & profile_streams_info = profile_streams_info_entry.second;

        // clang-format off
        if (collector.append<AggStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<ExchangeReceiverStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<ExchangeSenderStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<FilterStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<JoinStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<LimitStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<ProjectionStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<TableScanStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<TopNStatistics>(executor_id, profile_streams_info)) {}
        else if (collector.append<ExchangeReceiverStatistics>(executor_id, profile_streams_info)) {}
        else
        {
            throw TiFlashException(
                fmt::format("Unknown executor_id: {}", executor_id),
                Errors::Coprocessor::Internal);
        }
        // clang-format on
    }
    return collector.res;
}
} // namespace DB
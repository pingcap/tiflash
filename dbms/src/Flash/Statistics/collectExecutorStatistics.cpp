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
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
struct ExecutorStatisticsCollector
{
    Context & context;

    std::map<String, ExecutorStatisticsPtr> res;

    explicit ExecutorStatisticsCollector(Context & context_)
        : context(context_)
    {}

    template <typename T>
    bool append(const String & executor_id, const ProfileStreamsInfo & profile_streams_info)
    {
        if (T::hit(executor_id))
        {
            res[executor_id] = T::buildStatistics(executor_id, profile_streams_info, context);
            return true;
        }
        else
        {
            return false;
        }
    }
};
} // namespace

std::map<String, ExecutorStatisticsPtr> collectExecutorStatistics(Context & context)
{
    ExecutorStatisticsCollector collector{context};
    auto & dag_context = *context.getDAGContext();
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
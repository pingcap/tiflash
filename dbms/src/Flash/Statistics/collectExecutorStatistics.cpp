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
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
namespace
{
struct ExecutorStatisticsCollector
{
    DAGContext & dag_context;

    std::map<String, ExecutorStatisticsPtr> res;

    explicit ExecutorStatisticsCollector(DAGContext & dag_context_)
        : dag_context(dag_context_)
    {}

    template <typename... Ts>
    inline bool append(const String & executor_id, const tipb::Executor * executor)
    {
        assert(res.find(executor_id) == res.end());
        return (doAppend<Ts>(executor_id, executor) || ...);
    }

private:
    template <typename T>
    inline bool doAppend(const String & executor_id, const tipb::Executor * executor)
    {
        if (T::hit(executor_id))
        {
            res[executor_id] = std::make_shared<T>(executor, dag_context);
            return true;
        }
        return false;
    }
};
} // namespace

std::map<String, ExecutorStatisticsPtr> initExecutorStatistics(DAGContext & dag_context)
{
    ExecutorStatisticsCollector collector{dag_context};
    assert(dag_context.dag_request);
    const auto * dag_request = dag_context.dag_request;
    traverseExecutors(dag_request, [&](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        const auto & executor_id = executor.executor_id();
        if (!collector.append<
                AggStatistics,
                ExchangeReceiverStatistics,
                ExchangeSenderStatistics,
                FilterStatistics,
                JoinStatistics,
                LimitStatistics,
                ProjectionStatistics,
                TableScanStatistics,
                TopNStatistics>(executor_id, &executor))
        {
            throw TiFlashException(
                fmt::format("Unknown executor type, executor_id: {}", executor_id),
                Errors::Coprocessor::Internal);
        }
    });
    return collector.res;
}
} // namespace DB
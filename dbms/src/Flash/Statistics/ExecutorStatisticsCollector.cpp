#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/CommonExecutorImpl.h>
#include <Flash/Statistics/ExchangeReceiverImpl.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
void ExecutorStatisticsCollector::initialize(DAGContext * dag_context_)
{
    assert(dag_context_);
    dag_context = dag_context_;
    assert(dag_context->dag_request);
    traverseExecutors(dag_context->dag_request, [&](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        const auto & executor_id = executor.executor_id();
        if (!append<
                AggStatistics,
                ExchangeReceiverStatistics,
                ExchangeSenderStatistics,
                FilterStatistics,
                JoinStatistics,
                LimitStatistics,
                ProjectStatistics,
                TableScanStatistics,
                TopNStatistics>(executor_id, &executor))
        {
            throw TiFlashException(
                fmt::format("Unknown executor type, executor_id: {}", executor_id),
                Errors::Coprocessor::Internal);
        }
    });
}

void ExecutorStatisticsCollector::collectRuntimeDetails()
{
    assert(dag_context);
    assert(res.size() == dag_context->getProfileStreamsMap().size());
    for (const auto & entry : res)
    {
        entry.second->collectRuntimeDetail();
    }
}
} // namespace DB
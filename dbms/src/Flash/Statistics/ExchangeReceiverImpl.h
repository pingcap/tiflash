#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct ExchangeReceiverImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "ExchangeReceiver";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_exchange_receiver();
    }
};

template class ExecutorStatistics<ExchangeReceiverImpl>;
using ExchangeReceiverStatisticsBase = ExecutorStatistics<ExchangeReceiverImpl>;

class ExchangeReceiverStatistics : public ExchangeReceiverStatisticsBase
{
public:
    ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    std::vector<Int64> receiver_source_task_ids;
    size_t partition_num;

protected:
    void appendExtraJson(FmtBuffer &) const override;
};
} // namespace DB
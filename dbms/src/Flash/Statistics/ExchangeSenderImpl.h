#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct ExchangeSenderImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "ExchangeSender";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_exchange_sender();
    }
};

template class ExecutorStatistics<ExchangeSenderImpl>;
using ExchangeSenderStatisticsBase = ExecutorStatistics<ExchangeSenderImpl>;

class ExchangeSenderStatistics : public ExchangeSenderStatisticsBase
{
public:
    ExchangeSenderStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    UInt16 partition_num;
    tipb::ExchangeType exchange_type;
    std::vector<Int64> sender_target_task_ids;

protected:
    void appendExtraJson(FmtBuffer &) const override;
};
} // namespace DB

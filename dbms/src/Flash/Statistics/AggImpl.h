#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct AggImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Agg";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_aggregation();
    }
};
using AggStatisticsBase = ExecutorStatistics<AggImpl>;

class AggStatistics : public AggStatisticsBase
{
public:
    AggStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    size_t hash_table_rows = 0;

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;
};
} // namespace DB
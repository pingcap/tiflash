#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct JoinImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "Join";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_join();
    }
};

using JoinStatisticsBase = ExecutorStatistics<JoinImpl>;

class JoinStatistics : public JoinStatisticsBase
{
public:
    JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    size_t hash_table_bytes = 0;
    String build_side_child;

    size_t non_joined_outbound_rows = 0;
    size_t non_joined_outbound_blocks = 0;
    size_t non_joined_outbound_bytes = 0;

    UInt64 non_joined_execution_time_ns = 0;

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;
};
} // namespace DB
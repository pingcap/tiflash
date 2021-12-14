#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct JoinStatistics : public ExecutorStatistics
{
    size_t inbound_rows = 0;
    size_t inbound_blocks = 0;
    size_t inbound_bytes = 0;

    size_t hash_table_bytes = 0;

    UInt64 process_time_ns_for_build = 0;

    JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
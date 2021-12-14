#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>
#include <tipb/schema.pb.h>

#include <vector>

namespace DB
{
struct TableScanStatistics : public ExecutorStatistics
{
    TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;
};
} // namespace DB
#pragma once

#include <Flash/Coprocessor/ProfileStreamsInfo.h>
#include <common/types.h>

#include <memory>
#include <vector>

namespace DB
{
class Context;
class DAGContext;

struct ExecutorStatistics
{
    Int64 id;
    String type;

    std::vector<Int64> children;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    UInt64 execution_time_ns = 0;

    ExecutorStatistics(const String & executor_id, Context & context);

    virtual String toJson() const final;

    virtual ~ExecutorStatistics() = default;

protected:
    /// If not empty, start with ','
    virtual String extraToJson() const { return ""; }
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatistics>;
} // namespace DB
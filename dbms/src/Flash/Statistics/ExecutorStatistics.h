#pragma once

#include <Flash/Coprocessor/ProfileStreamsInfo.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class DAGContext;

struct ExecutorStatistics
{
    String id;
    String type;

    explicit ExecutorStatistics(const String & executor_id);

    virtual String toJson() const = 0;

    virtual ~ExecutorStatistics() = default;
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatistics>;
} // namespace DB
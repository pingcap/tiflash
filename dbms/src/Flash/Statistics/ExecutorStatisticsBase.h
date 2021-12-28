#pragma once

#include <common/types.h>

#include <memory>

struct BaseRuntimeStatistics
{
    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    UInt64 execution_time_ns = 0;
};

class ExecutorStatisticsBase
{
public:
    virtual String toJson() const = 0;

    virtual void collectRuntimeDetail() = 0;

    virtual ~ExecutorStatisticsBase() = default;

    BaseRuntimeStatistics getBaseRuntimeStatistics() const
    {
        return {outbound_rows, outbound_blocks, outbound_bytes, execution_time_ns};
    }

protected:
    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    UInt64 execution_time_ns = 0;
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatisticsBase>;
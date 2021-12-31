#pragma once

#include <common/types.h>

#include <memory>

namespace DB
{
struct BlockStreamProfileInfo;
struct BaseRuntimeStatistics
{
    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    UInt64 execution_time_ns = 0;

    void append(const BlockStreamProfileInfo &);
};

class ExecutorStatisticsBase
{
public:
    virtual String toJson() const = 0;

    virtual void collectRuntimeDetail() = 0;

    virtual ~ExecutorStatisticsBase() = default;

    const BaseRuntimeStatistics & getBaseRuntimeStatistics() const { return base; }

protected:
    BaseRuntimeStatistics base;
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatisticsBase>;
} // namespace DB
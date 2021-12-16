#pragma once

#include <common/types.h>

#include <memory>

class ExecutorStatisticsBase
{
public:
    virtual String toJson() const = 0;

    virtual void collectRuntimeDetail() = 0;

    virtual ~ExecutorStatisticsBase() = default;
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatisticsBase>;
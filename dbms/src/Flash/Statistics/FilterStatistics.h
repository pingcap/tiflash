#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct FilterStatistics : public ExecutorStatistics
{
    size_t inbound_rows = 0;
    size_t inbound_blocks = 0;
    size_t inbound_bytes = 0;

    FilterStatistics(const tipb::Executor * executor, Context & context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
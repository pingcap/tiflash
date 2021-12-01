#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct AggStatistics : public ExecutorStatistics
{
    size_t inbound_rows = 0;
    size_t inbound_blocks = 0;
    size_t inbound_bytes = 0;

    size_t hash_table_rows = 0;

    AggStatistics(const String & executor_id_, Context & context)
        : ExecutorStatistics(executor_id_, context)
    {}

    String extraToJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context);
};
} // namespace DB
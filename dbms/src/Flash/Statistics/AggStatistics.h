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

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    size_t hash_table_rows = 0;

    explicit AggStatistics(const String & executor_id_)
        : ExecutorStatistics(executor_id_)
    {}

    String toJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
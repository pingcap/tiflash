#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct JoinStatistics : public ExecutorStatistics
{
    size_t probe_inbound_rows = 0;
    size_t probe_inbound_blocks = 0;
    size_t probe_inbound_bytes = 0;

    size_t probe_outbound_rows = 0;
    size_t probe_outbound_blocks = 0;
    size_t probe_outbound_bytes = 0;

    size_t hash_table_bytes = 0;

    UInt64 process_time_for_build = 0;

    explicit JoinStatistics(const String & executor_id_)
        : ExecutorStatistics(executor_id_)
    {}

    String toJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>

#include <unordered_set>

namespace DB
{
struct JoinStatistics;

using JoinStatisticsPtr = std::shared_ptr<JoinStatistics>;

struct JoinStatistics
{
    const String & executor_id;

    size_t probe_inbound_rows = 0;
    size_t probe_inbound_blocks = 0;
    size_t probe_inbound_bytes = 0;

    size_t probe_outbound_rows = 0;
    size_t probe_outbound_blocks = 0;
    size_t probe_outbound_bytes = 0;

    size_t hash_table_bytes = 0;

    explicit JoinStatistics(const String & executor_id_)
        : executor_id(executor_id_)
    {}

    String toString() const;

    static bool isHit(const String & executor_id)
    {
        return startsWith(executor_id, "Join_");
    }

    static JoinStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
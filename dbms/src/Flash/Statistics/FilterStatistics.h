#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct FilterStatistics;

using FilterStatisticsPtr = std::shared_ptr<FilterStatistics>;

struct FilterStatistics
{
    const String & executor_id;

    size_t inbound_rows = 0;
    size_t inbound_blocks = 0;
    size_t inbound_bytes = 0;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    explicit FilterStatistics(const String & executor_id_)
        : executor_id(executor_id_)
    {}

    String toString() const;

    static bool hit(const String & executor_id)
    {
        return startsWith(executor_id, "Selection_");
    }

    static FilterStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
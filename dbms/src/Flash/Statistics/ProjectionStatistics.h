#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct ProjectionStatistics : public ExecutorStatistics
{
    ProjectionStatistics(const String & executor_id_, Context & context)
        : ExecutorStatistics(executor_id_, context)
    {}

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context);
};
} // namespace DB
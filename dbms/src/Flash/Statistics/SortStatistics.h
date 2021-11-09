#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <common/types.h>

#include <unordered_set>

struct SortStatistics;

using SortStatisticsPtr = std::shared_ptr<SortStatistics>;

struct SortStatistics
{
    const String & executor_id;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    explicit SortStatistics(const String & executor_id_)
        : executor_id(executor_id_)
    {}

    static bool hit(const String & executor_id)
    {
        return startsWith(executor_id, "HashAgg_") || startsWith(executor_id, "StreamAgg_");
    }

    static SortStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context)
    {
        SortStatisticsPtr statistics = std::make_shared<SortStatistics>(executor_id);
        visitProfileStreamsInfo(
            profile_streams_info,
            [&](const BlockStreamProfileInfo & profile_info) {
                statistics->outbound_rows += profile_info.rows;
                statistics->outbound_blocks += profile_info.blocks;
                statistics->outbound_bytes += profile_info.bytes;
            });
        return statistics;
    }
};
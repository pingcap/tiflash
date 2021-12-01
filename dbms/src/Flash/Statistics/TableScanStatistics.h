#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>
#include <tipb/schema.pb.h>

#include <vector>

namespace DB
{
struct TableScanStatistics : public ExecutorStatistics
{
    String db;
    String table;

    std::vector<tipb::KeyRange> ranges;

    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    TableScanStatistics(const String & executor_id_, Context & context)
        : ExecutorStatistics(executor_id_, context)
    {}

    String extraToJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context);
};
} // namespace DB
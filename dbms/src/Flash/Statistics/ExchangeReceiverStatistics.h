#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct ExchangeReceiverStatistics : public ExecutorStatistics
{
    std::vector<Int64> receiver_source_task_ids;

    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    size_t partition_num;

    ExchangeReceiverStatistics(const String & executor_id_, Context & context)
        : ExecutorStatistics(executor_id_, context)
    {}

    String extraToJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context);
};
} // namespace DB
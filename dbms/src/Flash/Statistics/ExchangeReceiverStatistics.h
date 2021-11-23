#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct ExchangeReceiverStatistics : public ExecutorStatistics
{
    std::vector<Int64> downstream_task_ids;

    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    size_t partition_num;

    explicit ExchangeReceiverStatistics(const String & executor_id_)
        : ExecutorStatistics(executor_id_)
    {}

    String toJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
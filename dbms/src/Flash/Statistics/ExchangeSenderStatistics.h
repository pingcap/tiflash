#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
struct ExchangeSenderStatistics : public ExecutorStatistics
{
    UInt16 partition_num;

    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    tipb::ExchangeType exchange_type;

    explicit ExchangeSenderStatistics(const String & executor_id_)
        : ExecutorStatistics(executor_id_)
    {}

    String toJson() const override;

    static bool hit(const String & executor_id);

    static ExecutorStatisticsPtr buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context);
};
} // namespace DB
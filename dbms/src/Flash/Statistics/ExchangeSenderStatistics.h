#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
struct MPPTunnelDetail
{
    String tunnel_id;
    Int64 sender_target_task_id;
    bool is_local;
    ConnectionProfileInfo connection_profile_info;

    String toJson() const;
};

struct ExchangeSenderStatistics : public ExecutorStatistics
{
    UInt16 partition_num;

    tipb::ExchangeType exchange_type;

    std::vector<Int64> sender_target_task_ids;

    std::vector<MPPTunnelDetail> mpp_tunnel_details;

    ExchangeSenderStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct ExchangeReceiveDetail
{
    Int64 receiver_source_task_id;
    ConnectionProfileInfo connection_profile_info;

    String toJson() const;
};

struct ExchangeReceiverStatistics : public ExecutorStatistics
{
    std::vector<Int64> receiver_source_task_ids;

    size_t partition_num;

    std::vector<ExchangeReceiveDetail> exchange_receive_details;

    ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
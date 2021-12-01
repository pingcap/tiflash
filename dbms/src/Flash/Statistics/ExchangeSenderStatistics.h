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

    std::vector<Int64> sender_target_task_ids;

    ExchangeSenderStatistics(const tipb::Executor * executor, Context & context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
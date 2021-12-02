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

    size_t partition_num;

    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    ExchangeReceiverStatistics(const tipb::Executor * executor, Context & context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
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
    std::vector<ConnectionProfileInfoPtr> connection_profile_infos;

    TableScanStatistics(const tipb::Executor * executor, Context & context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;
};
} // namespace DB
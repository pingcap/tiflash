#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <common/types.h>
#include <tipb/schema.pb.h>

#include <vector>

namespace DB
{
struct TableScanDetail
{
    bool is_local;
    ConnectionProfileInfo connection_profile_info;

    String toJson() const;
};

struct TableScanStatistics : public ExecutorStatistics
{
    TableScanDetail local_table_scan_detail;
    TableScanDetail cop_table_scan_detail;

    TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

    static bool hit(const String & executor_id);

    void collectRuntimeDetail() override;

protected:
    String extraToJson() const override;
};
} // namespace DB
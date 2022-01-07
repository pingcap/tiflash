#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct TableScanDetail : public ConnectionProfileInfo
{
    bool is_local;

    explicit TableScanDetail(bool is_local_)
        : is_local(is_local_)
    {}

    String toJson() const;
};

struct TableScanImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "TableScan";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_tbl_scan() || executor->has_partition_table_scan();
    }
};
using TableScanStatisticsBase = ExecutorStatistics<TableScanImpl>;

class TableScanStatistics : public TableScanStatisticsBase
{
public:
    TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    TableScanDetail local_table_scan_detail{true};
    TableScanDetail cop_table_scan_detail{false};

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;
};
} // namespace DB
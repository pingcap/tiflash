#pragma once

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalTableScan : public PhysicalLeaf
{
public:
    static PhysicalPlanPtr build(
        Context & context,
        const tipb::Executor * executor,
        const String & executor_id);

    PhysicalTableScan(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const TiDBTableScan & tidb_table_scan_)
        : PhysicalLeaf(executor_id_, PlanType::TableScan, schema_)
        , tidb_table_scan(tidb_table_scan_)
    {
    }

    void pushDownFilter(const tipb::Selection & selection);

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    // record profile streams by itself.
    void transform(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

private:
    TiDBTableScan tidb_table_scan;

    // for pushed down filter
    String pushed_down_filter_id;
    std::vector<const tipb::Expr *> pushed_down_conditions;
};
} // namespace DB
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Flash/Coprocessor/TableScanInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalTableScan::build(
    Context & context,
    const tipb::Executor * executor,
    const String & executor_id)
{
    TiDBTableScan table_scan(executor, executor_id, *context.getDAGContext());
    context.getDAGContext()->table_scan_executor_id = executor_id;
    DAGPipeline pipeline;
    TableScanInterpreterHelper::handleTableScan(
        context,
        table_scan,
        "",
        {},
        pipeline,
        max_streams);
}

void PhysicalTableScan::transform(DAGPipeline & pipeline, const Context & /*context*/, size_t /*max_streams*/)
{
    pipeline.streams = streams;
}

void PhysicalTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalTableScan::getSampleBlock() const
{
    return streams.back()->getHeader();
}
} // namespace DB
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/TableScanInterpreterHelper.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalTableScan::build(
    Context & context,
    const tipb::Executor * executor,
    const String & executor_id)
{
    TiDBTableScan tidb_table_scan(executor, executor_id, *context.getDAGContext());
    auto storages_with_structure_lock = TableScanInterpreterHelper::getAndLockStorages(context, tidb_table_scan);
    NamesAndTypes schema = TableScanInterpreterHelper::getSchemaForTableScan(context, tidb_table_scan, storages_with_structure_lock);

    auto physical_table_scan = std::make_shared<PhysicalTableScan>(
        executor_id,
        schema,
        tidb_table_scan,
        storages_with_structure_lock);
    return physical_table_scan;
}

PhysicalTableScan::PhysicalTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const TiDBTableScan & tidb_table_scan_,
    const IDsAndStorageWithStructureLocks & storages_with_structure_lock_)
    : PhysicalLeaf(executor_id_, PlanType::TableScan, schema_)
    , tidb_table_scan(tidb_table_scan_)
    , storages_with_structure_lock(storages_with_structure_lock_)
    , sample_block(PhysicalPlanHelper::constructBlockFromSchema(schema_))
{}

void PhysicalTableScan::pushDownFilter(const String filter_executor_id, const tipb::Selection & selection)
{
    assert(pushed_down_filter_id.empty() && pushed_down_conditions.empty());
    pushed_down_filter_id = filter_executor_id;
    assert(!pushed_down_filter_id.empty());
    for (const auto & condition : selection.conditions())
        pushed_down_conditions.push_back(&condition);
    assert(!pushed_down_conditions.empty());
}

void PhysicalTableScan::transform(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    context.getDAGContext()->table_scan_executor_id = executor_id;

    TableScanInterpreterHelper::handleTableScan(
        context,
        tidb_table_scan,
        // to release the holder of table locks.
        // too hack...
        std::move(storages_with_structure_lock),
        schema,
        pushed_down_filter_id,
        pushed_down_conditions,
        pipeline,
        max_streams);
    storages_with_structure_lock.clear();

    assert(pipeline.hasMoreThanOneStream());
    const auto & header = pipeline.firstStream()->getHeader();

    // to make sure that pipeline.header == schema
    const auto & logger = context.getDAGContext()->log;
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(header, context);
    const auto header_names = header.getNames();
    assert(header_names.size() == schema.size());
    FinalizeHelper::checkSchemaContainsSampleBlock(schema, header);
    NamesWithAliases project_aliases;
    for (size_t i = 0; i < header_names.size(); ++i)
        project_aliases.emplace_back(header_names[i], schema[i].name);
    project_actions->add(ExpressionAction::project(project_aliases));
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project_actions, logger->identifier()); });
}

void PhysicalTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalTableScan::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalMockTableScan.h>
#include <Flash/Planner/Plans/PhysicalTableScan.h>
#include <Flash/Planner/Plans/PhysicalTopN.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationTopN.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
DM::MultiStageLateMaterializationTopNDescriptionPtr tryBuildMultiStageLateMaterializationTopN(
    const tipb::TopN & top_n,
    const TiDB::ColumnInfos & table_scan_columns,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
    bool has_filter_conditions,
    const LoggerPtr & log)
{
    auto disable = [&](const String & reason) -> DM::MultiStageLateMaterializationTopNDescriptionPtr {
        LOG_DEBUG(log, "Disable TopN-enhanced multi-stage late materialization, reason={}", reason);
        return nullptr;
    };

    if (!has_filter_conditions)
        return disable("no residual filter conditions");
    if (pushed_down_filters.empty())
        return disable("no stage0 pushed down filters");
    if (top_n.limit() == 0)
        return disable("topk is zero");
    if (top_n.limit() > DM::multi_stage_late_materialization_topn_max_topk)
        return disable(fmt::format("topk is too large: {}", top_n.limit()));
    if (top_n.order_by_size() > static_cast<int>(DM::multi_stage_late_materialization_topn_max_order_by_columns))
        return disable(fmt::format("too many order by columns: {}", top_n.order_by_size()));

    auto desc = std::make_shared<DM::MultiStageLateMaterializationTopNDescription>();
    desc->topk = top_n.limit();
    desc->order_by_columns.reserve(top_n.order_by_size());

    for (const auto & by_item : top_n.order_by())
    {
        if (!isColumnExpr(by_item.expr()))
            return disable("order by expression is not direct ColumnRef");

        const auto column_index = decodeDAGInt64(by_item.expr().val());
        if (column_index < 0 || column_index >= static_cast<Int64>(table_scan_columns.size()))
            return disable(fmt::format(
                "order by ColumnRef index out of range, index={}, table_scan_column_size={}",
                column_index,
                table_scan_columns.size()));

        const auto & column = table_scan_columns[column_index];
        if (column.hasGeneratedColumnFlag())
            return disable(fmt::format("order by generated column is unsupported, column_id={}", column.id));
        if (column.id == ExtraTableIDColumnID || column.id == ExtraCommitTSColumnID)
            return disable(fmt::format("order by virtual column is unsupported, column_id={}", column.id));

        desc->order_by_columns.push_back(
            DM::MultiStageLateMaterializationTopNOrderByColumn{column.id, by_item.desc() ? -1 : 1});
    }

    return desc;
}

void tryAttachMultiStageLateMaterializationTopN(
    const tipb::TopN & top_n,
    const PhysicalPlanNodePtr & child,
    const LoggerPtr & log)
{
    if (auto table_scan = std::dynamic_pointer_cast<PhysicalTableScan>(child))
    {
        auto topn = tryBuildMultiStageLateMaterializationTopN(
            top_n,
            table_scan->getTiDBTableScan().getColumns(),
            table_scan->getTiDBTableScan().getPushedDownFilters(),
            table_scan->hasFilterConditions(),
            log);
        table_scan->setMultiStageLateMaterializationTopN(topn);
        return;
    }

    if (auto table_scan = std::dynamic_pointer_cast<PhysicalMockTableScan>(child))
    {
        auto topn = tryBuildMultiStageLateMaterializationTopN(
            top_n,
            table_scan->getTableScanColumns(),
            table_scan->getPushedDownFilters(),
            table_scan->hasFilterConditions(),
            log);
        table_scan->setMultiStageLateMaterializationTopN(topn);
        return;
    }

    LOG_DEBUG(log, "Disable TopN-enhanced multi-stage late materialization, reason=child is not direct table scan");
}
} // namespace

PhysicalPlanNodePtr PhysicalTopN::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::TopN & top_n,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    if (unlikely(top_n.order_by_size() == 0))
    {
        //should not reach here
        throw TiFlashException("TopN executor without order by exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_sort_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    auto order_columns = analyzer.buildOrderColumns(before_sort_actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());
    tryAttachMultiStageLateMaterializationTopN(top_n, child, log);

    auto physical_top_n = std::make_shared<PhysicalTopN>(
        executor_id,
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        order_descr,
        before_sort_actions,
        top_n.limit());
    return physical_top_n;
}

void PhysicalTopN::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    executeExpression(pipeline, before_sort_actions, log, "before TopN");

    orderStreams(pipeline, max_streams, order_descr, limit, false, context, log);
}

void PhysicalTopN::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    executeExpression(exec_context, group_builder, before_sort_actions, log);

    // If the `limit` is very large, using a `final sort` can avoid outputting excessively large amounts of data.
    // TODO find a suitable threshold is necessary; 10000 is just a value picked without much consideration.
    if (group_builder.concurrency() * limit <= 10000)
    {
        executeLocalSort(exec_context, group_builder, order_descr, limit, false, context, log);
    }
    else
    {
        executeFinalSort(exec_context, group_builder, order_descr, limit, context, log);
        if (is_restore_concurrency)
            restoreConcurrency(
                exec_context,
                group_builder,
                concurrency,
                context.getSettingsRef().max_buffered_bytes_in_executor,
                log);
    }
}

void PhysicalTopN::finalizeImpl(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.emplace_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB

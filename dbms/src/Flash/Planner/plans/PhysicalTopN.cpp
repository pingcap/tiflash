#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalTopN::build(
    const Context & context,
    const String & executor_id,
    const tipb::TopN & top_n,
    PhysicalPlanPtr child)
{
    assert(child);

    if (top_n.order_by_size() == 0)
    {
        //should not reach here
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_sort_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);

    auto order_columns = analyzer.buildOrderColumns(before_sort_actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    auto physical_top_n = std::make_shared<PhysicalTopN>(executor_id, child->getSchema(), order_descr, before_sort_actions, top_n.limit());
    physical_top_n->appendChild(child);
    return physical_top_n;
}

void PhysicalTopN::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;
    const Settings & settings = context.getSettingsRef();

    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, before_sort_actions, logger->identifier());

        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, logger->identifier(), limit);
        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, logger);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        logger->identifier());
}

void PhysicalTopN::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.push_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
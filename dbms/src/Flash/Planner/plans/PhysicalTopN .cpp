#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalTopN::transform(DAGPipeline & pipeline, const Context & context, size_t max_streams)
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
    prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
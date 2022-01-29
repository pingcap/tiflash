#include <Common/LogWithPrefix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalTopN::transform(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    const LogWithPrefixPtr & logger = context.getDAGContext()->log;
    const Settings & settings = context.getSettingsRef();

    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, before_sort_actions, logger);

        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, logger, limit);
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
        logger);
}

bool PhysicalTopN::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(schema.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.push_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    if (child->finalize(before_sort_actions->getRequiredColumns()))
    {
        size_t columns_from_previous = child->getSampleBlock().columns();
        if (!before_sort_actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > before_sort_actions->getRequiredColumnsWithTypes().size())
            before_sort_actions->prependProjectInput();
    }

    return true;
}

const Block & PhysicalTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}
} // namespace DB
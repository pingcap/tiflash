#include <Common/LogWithPrefix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/ExchangeSender.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalExchangeSender::transform(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    auto & dag_context = *context.getDAGContext();
    const LogWithPrefixPtr & logger = dag_context->log;
    restoreConcurrency(pipeline, dag_context.final_concurrency, logger);

    /// only run in MPP
    assert(dag_context.isMPPTask() && dag_context.tunnel_set != nullptr);
    int stream_id = 0;
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(
            context.getDAGContext()->tunnel_set,
            partition_col_id,
            collators,
            exchange_type,
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            stream_id++ == 0, /// only one stream needs to sending execution summaries for the last response
            dag_context);
        stream = std::make_shared<ExchangeSender>(stream, std::move(response_writer), logger);
    });
    recordProfileStreams(pipeline, dag_context);
}

bool PhysicalExchangeSender::finalize(const Names & parent_require)
{
    return child->finalize(parent_require);
}

const Block & PhysicalExchangeSender::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
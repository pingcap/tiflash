#include <Common/Logger.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalExchangeSender::build(
    const String & executor_id,
    const tipb::ExchangeSender & exchange_sender,
    PhysicalPlanPtr child)
{
    assert(child);

    // Can't use auto [partition_col_ids, partition_col_collators],
    // because of `Structured bindings cannot be captured by lambda expressions. (until C++20)`
    // https://en.cppreference.com/w/cpp/language/structured_binding
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators partition_col_collators;
    std::tie(partition_col_ids, partition_col_collators) = ExchangeSenderInterpreterHelper::genPartitionColIdsAndCollators(exchange_sender);

    auto physical_exchange_sender = std::make_shared<PhysicalExchangeSender>(
        executor_id,
        child->getSchema(),
        partition_col_ids,
        partition_col_collators,
        exchange_sender.tp());
    physical_exchange_sender->appendChild(child);
    return physical_exchange_sender;
}

void PhysicalExchangeSender::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
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
        stream = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), logger->identifier());
    });
}

void PhysicalExchangeSender::finalize(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalExchangeSender::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
#include <Common/TiFlashException.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalExchangeReceiver.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TypeMapping.h>
#include <fmt/format.h>

namespace DB
{
PhysicalPlanPtr PhysicalExchangeReceiver::build(
    const Context & context,
    const String & executor_id)
{
    const auto & mpp_exchange_receiver_map = context.getDAGContext()->getMPPExchangeReceiverMap();

    auto it = mpp_exchange_receiver_map.find(executor_id);
    if (unlikely(it == mpp_exchange_receiver_map.end()))
        throw TiFlashException(
            fmt::format("Can not find exchange receiver for {}", executor_id),
            Errors::Coprocessor::Internal);

    const auto & mpp_exchange_receiver = it->second;
    NamesAndTypes schema;
    for (const auto & col : mpp_exchange_receiver->getOutputSchema())
    {
        auto tp = getDataTypeByColumnInfoForComputingLayer(col.second);
        schema.emplace_back(col.first, tp);
    }
    auto physical_exchange_receiver = std::make_shared<PhysicalExchangeReceiver>(
        executor_id,
        schema,
        mpp_exchange_receiver);
    return physical_exchange_receiver;
}

void PhysicalExchangeReceiver::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[executor_id];
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(mpp_exchange_receiver, logger->identifier(), executor_id);
        exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, logger->identifier());
        pipeline.streams.push_back(stream);
    }
}

void PhysicalExchangeReceiver::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalExchangeReceiver::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
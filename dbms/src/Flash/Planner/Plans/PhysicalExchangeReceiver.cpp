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

#include <Common/TiFlashException.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalExchangeReceiver.h>
#include <Interpreters/Context.h>
#include <Operators/ExchangeReceiverSourceOp.h>
#include <TiDB/Decode/TypeMapping.h>
#include <fmt/format.h>

namespace DB
{
PhysicalExchangeReceiver::PhysicalExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const FineGrainedShuffle & fine_grained_shuffle,
    const String & req_id,
    const Block & sample_block_,
    const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_)
    : PhysicalLeaf(executor_id_, PlanType::ExchangeReceiver, schema_, fine_grained_shuffle, req_id)
    , sample_block(sample_block_)
    , mpp_exchange_receiver(mpp_exchange_receiver_)
{}

PhysicalPlanNodePtr PhysicalExchangeReceiver::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const FineGrainedShuffle & fine_grained_shuffle)
{
    auto mpp_exchange_receiver = context.getDAGContext()->getMPPExchangeReceiver(executor_id);
    if (unlikely(mpp_exchange_receiver == nullptr))
        throw TiFlashException(
            fmt::format("Can not find exchange receiver for {}", executor_id),
            Errors::Planner::Internal);

    NamesAndTypes schema = toNamesAndTypes(mpp_exchange_receiver->getOutputSchema());
    auto physical_exchange_receiver = std::make_shared<PhysicalExchangeReceiver>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        Block(schema),
        mpp_exchange_receiver);
    return physical_exchange_receiver;
}

void PhysicalExchangeReceiver::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    RUNTIME_CHECK(pipeline.streams.empty());

    auto & dag_context = *context.getDAGContext();
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[executor_id];

    String extra_info = "squashing after exchange receiver";
    size_t stream_count = max_streams;
    if (fine_grained_shuffle.enabled())
    {
        extra_info += ", " + String(enableFineGrainedShuffleExtraInfo);
        stream_count = std::min(max_streams, fine_grained_shuffle.stream_count);
    }

    for (size_t i = 0; i < stream_count; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(
            mpp_exchange_receiver,
            log->identifier(),
            execId(),
            /*stream_id=*/fine_grained_shuffle.enabled() ? i : 0);
        exchange_receiver_io_input_streams.push_back(stream);
        stream->setExtraInfo(extra_info);
        pipeline.streams.push_back(stream);
    }
}

void PhysicalExchangeReceiver::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    if (fine_grained_shuffle.enabled())
        concurrency = std::min(concurrency, fine_grained_shuffle.stream_count);

    for (size_t partition_id = 0; partition_id < concurrency; ++partition_id)
    {
        group_builder.addConcurrency(std::make_unique<ExchangeReceiverSourceOp>(
            exec_context,
            log->identifier(),
            mpp_exchange_receiver,
            /*stream_id=*/fine_grained_shuffle.enabled() ? partition_id : 0));
    }
    context.getDAGContext()->addInboundIOProfileInfos(executor_id, group_builder.getCurIOProfileInfos());
}

void PhysicalExchangeReceiver::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalExchangeReceiver::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB

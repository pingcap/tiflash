// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalExchangeReceiver.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TypeMapping.h>
#include <fmt/format.h>

namespace DB
{
PhysicalExchangeReceiver::PhysicalExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const Block & sample_block_,
    const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_)
    : PhysicalLeaf(executor_id_, PlanType::ExchangeReceiver, schema_, req_id)
    , sample_block(sample_block_)
    , mpp_exchange_receiver(mpp_exchange_receiver_)
{}

PhysicalPlanNodePtr PhysicalExchangeReceiver::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log)
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
        log->identifier(),
        Block(schema),
        mpp_exchange_receiver);
    return physical_exchange_receiver;
}

void PhysicalExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());

    auto & dag_context = *context.getDAGContext();
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[executor_id];

    const bool enable_fine_grained_shuffle = enableFineGrainedShuffle(mpp_exchange_receiver->getFineGrainedShuffleStreamCount());
    String extra_info = "squashing after exchange receiver";
    size_t stream_count = max_streams;
    if (enable_fine_grained_shuffle)
    {
        extra_info += ", " + String(enableFineGrainedShuffleExtraInfo);
        stream_count = std::min(max_streams, mpp_exchange_receiver->getFineGrainedShuffleStreamCount());
    }

    for (size_t i = 0; i < stream_count; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(mpp_exchange_receiver,
                                                                                   log->identifier(),
                                                                                   execId(),
                                                                                   /*stream_id=*/enable_fine_grained_shuffle ? i : 0);
        exchange_receiver_io_input_streams.push_back(stream);
        stream->setExtraInfo(extra_info);
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

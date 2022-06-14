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
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Mpp/ExchangeReceiver.h>
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
    const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_)
    : PhysicalLeaf(executor_id_, PlanType::ExchangeReceiver, schema_, req_id)
    , mpp_exchange_receiver(mpp_exchange_receiver_)
    , sample_block(PhysicalPlanHelper::constructBlockFromSchema(schema_))
{}

PhysicalPlanPtr PhysicalExchangeReceiver::build(
    const Context & context,
    const String & executor_id,
    const String & req_id)
{
    const auto & mpp_exchange_receiver_map = context.getDAGContext()->getMPPExchangeReceiverMap();

    auto it = mpp_exchange_receiver_map.find(executor_id);
    if (unlikely(it == mpp_exchange_receiver_map.end()))
        throw TiFlashException(
            fmt::format("Can not find exchange receiver for {}", executor_id),
            Errors::Planner::Internal);

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
        req_id,
        mpp_exchange_receiver);
    return physical_exchange_receiver;
}

void PhysicalExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    auto & dag_context = *context.getDAGContext();
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[query_block.source_name];
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(it->second, log->identifier(), executor_id);
        exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, log->identifier());
        stream->setExtraInfo("squashing after exchange receiver");
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

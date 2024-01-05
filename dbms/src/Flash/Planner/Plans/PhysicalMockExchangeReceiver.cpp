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

#include <DataStreams/MockExchangeReceiverInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalMockExchangeReceiver.h>
#include <Interpreters/Context.h>
#include <Operators/BlockInputStreamSourceOp.h>

namespace DB
{
PhysicalMockExchangeReceiver::PhysicalMockExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const FineGrainedShuffle & fine_grained_shuffle_,
    const String & req_id,
    const Block & sample_block_,
    const BlockInputStreams & mock_streams_,
    size_t source_num_)
    : PhysicalLeaf(executor_id_, PlanType::MockExchangeReceiver, schema_, fine_grained_shuffle_, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
    , source_num(source_num_)
{}

PhysicalPlanNodePtr PhysicalMockExchangeReceiver::build(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::ExchangeReceiver & exchange_receiver,
    const FineGrainedShuffle & fine_grained_shuffle)
{
    auto [schema, mock_streams] = mockSchemaAndStreamsForExchangeReceiver(
        context,
        executor_id,
        log,
        exchange_receiver,
        fine_grained_shuffle.stream_count);

    auto physical_mock_exchange_receiver = std::make_shared<PhysicalMockExchangeReceiver>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        Block(schema),
        mock_streams,
        static_cast<size_t>(exchange_receiver.encoded_task_meta_size()));
    return physical_mock_exchange_receiver;
}

void PhysicalMockExchangeReceiver::buildBlockInputStreamImpl(
    DAGPipeline & pipeline,
    Context & /*context*/,
    size_t /*max_streams*/)
{
    RUNTIME_CHECK(pipeline.streams.empty());
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalMockExchangeReceiver::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    for (auto & mock_stream : mock_streams)
        group_builder.addConcurrency(
            std::make_unique<BlockInputStreamSourceOp>(exec_context, log->identifier(), mock_stream));
}

void PhysicalMockExchangeReceiver::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalMockExchangeReceiver::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB

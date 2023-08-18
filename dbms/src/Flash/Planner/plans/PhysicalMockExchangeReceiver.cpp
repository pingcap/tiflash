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
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalMockExchangeReceiver.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
std::pair<NamesAndTypes, BlockInputStreams> mockSchemaAndStreams(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::ExchangeReceiver & exchange_receiver)
{
    NamesAndTypes schema;
    BlockInputStreams mock_streams;

    auto & dag_context = *context.getDAGContext();
    size_t max_streams = dag_context.initialize_concurrency;
    assert(max_streams > 0);

    if (!context.mockStorage().exchangeExists(executor_id))
    {
        /// build with default blocks.
        for (size_t i = 0; i < max_streams; ++i)
            // use max_block_size / 10 to determine the mock block's size
            mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(exchange_receiver, context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
        for (const auto & col : mock_streams.back()->getHeader())
            schema.emplace_back(col.name, col.type);
    }
    else
    {
        /// build from user input blocks.
        auto [names_and_types, mock_exchange_streams] = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, log, executor_id);
        schema = std::move(names_and_types);
        mock_streams.insert(mock_streams.end(), mock_exchange_streams.begin(), mock_exchange_streams.end());
    }

    assert(!schema.empty());
    assert(!mock_streams.empty());

    return {std::move(schema), std::move(mock_streams)};
}
} // namespace

PhysicalMockExchangeReceiver::PhysicalMockExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const Block & sample_block_,
    const BlockInputStreams & mock_streams_,
    size_t source_num_)
    : PhysicalLeaf(executor_id_, PlanType::MockExchangeReceiver, schema_, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
    , source_num(source_num_)
{}

PhysicalPlanNodePtr PhysicalMockExchangeReceiver::build(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::ExchangeReceiver & exchange_receiver)
{
<<<<<<< HEAD:dbms/src/Flash/Planner/plans/PhysicalMockExchangeReceiver.cpp
    assert(context.isExecutorTest());

    auto [schema, mock_streams] = mockSchemaAndStreams(context, executor_id, log, exchange_receiver);
=======
    auto [schema, mock_streams] = mockSchemaAndStreamsForExchangeReceiver(
        context,
        executor_id,
        log,
        exchange_receiver,
        fine_grained_shuffle.stream_count);
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Planner/Plans/PhysicalMockExchangeReceiver.cpp

    auto physical_mock_exchange_receiver = std::make_shared<PhysicalMockExchangeReceiver>(
        executor_id,
        schema,
        log->identifier(),
        Block(schema),
        mock_streams,
        static_cast<size_t>(exchange_receiver.encoded_task_meta_size()));
    return physical_mock_exchange_receiver;
}

<<<<<<< HEAD:dbms/src/Flash/Planner/plans/PhysicalMockExchangeReceiver.cpp
void PhysicalMockExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
=======
void PhysicalMockExchangeReceiver::buildBlockInputStreamImpl(
    DAGPipeline & pipeline,
    Context & /*context*/,
    size_t /*max_streams*/)
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Planner/Plans/PhysicalMockExchangeReceiver.cpp
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

<<<<<<< HEAD:dbms/src/Flash/Planner/plans/PhysicalMockExchangeReceiver.cpp
=======
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

>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Planner/Plans/PhysicalMockExchangeReceiver.cpp
void PhysicalMockExchangeReceiver::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalMockExchangeReceiver::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB

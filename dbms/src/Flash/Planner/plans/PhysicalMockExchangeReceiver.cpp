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

#include <DataStreams/MockExchangeReceiverInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalMockExchangeReceiver.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalMockExchangeReceiver::PhysicalMockExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const Block & sample_block_,
    const BlockInputStreams & mock_streams_)
    : PhysicalLeaf(executor_id_, PlanType::MockExchangeReceiver, schema_, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
{}

PhysicalPlanPtr PhysicalMockExchangeReceiver::build(
    Context & context,
    const String & executor_id,
    const String & req_id,
    const tipb::ExchangeReceiver & exchange_receiver)
{
    NamesAndTypes schema;
    BlockInputStreams mock_streams;
    auto & dag_context = *context.getDAGContext();
    size_t max_streams = dag_context.initialize_concurrency;
    if (dag_context.columnsForTestEmpty() || dag_context.columnsForTest(executor_id).empty())
    {
        for (size_t i = 0; i < max_streams; ++i)
            // use max_block_size / 10 to determine the mock block's size
            mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(exchange_receiver, context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
        for (const auto & col : mock_streams.back()->getHeader())
            schema.emplace_back(col.name, col.type);
    }
    else
    {
        auto [names_and_types, mock_exchange_streams] = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, dag_context.log, executor_id);
        schema = std::move(names_and_types);
        mock_streams.insert(mock_streams.end(), mock_exchange_streams.begin(), mock_exchange_streams.end());
    }
    assert(!schema.empty());
    assert(!mock_streams.empty());

    auto physical_mock_exchange_receiver = std::make_shared<PhysicalMockExchangeReceiver>(
        executor_id,
        schema,
        req_id,
        PhysicalPlanHelper::constructBlockFromSchema(schema),
        mock_streams);
    return physical_mock_exchange_receiver;
}

void PhysicalMockExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalMockExchangeReceiver::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalMockExchangeReceiver::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB

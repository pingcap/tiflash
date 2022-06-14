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
#include <Flash/Planner/plans/PhysicalMockExchangeReceiver.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalMockExchangeReceiver::PhysicalMockExchangeReceiver(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const BlockInputStreams & mock_streams_)
    : PhysicalLeaf(executor_id_, PlanType::ExchangeReceiver, schema_, req_id)
    , mock_streams(mock_streams_)
{}

PhysicalPlanPtr PhysicalMockExchangeReceiver::build(
    const Context & context,
    const String & executor_id,
    const String & req_id,
    const tipb::ExchangeReceiver & exchange_receiver)
{
    NamesAndTypes schema;
    BlockInputStreams mock_streams;
    size_t max_streams = context.getDAGContext().initialize_concurrency;
    if (context.getDAGContext()->columnsForTestEmpty() || context.getDAGContext()->columnsForTest(executor_id).empty())
    {
        for (size_t i = 0; i < max_streams; ++i)
            // use max_block_size / 10 to determine the mock block's size
            mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(exchange_receiver, context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
        for (const auto & col : mock_streams.back()->getHeader())
            schema.emplace_back(col.name, col.type);
    }
    else
    {
        std::tie(schema, mock_streams) = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, log, executor_id);
    }

    auto physical_mock_exchange_receiver = std::make_shared<PhysicalMockExchangeReceiver>(
        executor_id,
        schema,
        req_id,
        mock_streams);
    return physical_mock_exchange_receiver;
}

void PhysicalExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalExchangeReceiver::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalExchangeReceiver::getSampleBlock() const
{
    return mock_streams.back()->getHeader();
}
} // namespace DB

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
#include <Flash/Coprocessor/DAGContext.h>
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
    const tipb::ExchangeReceiver & exchange_receiver,
    size_t fine_grained_stream_count)
{
    NamesAndTypes schema;
    BlockInputStreams mock_streams;

    auto & dag_context = *context.getDAGContext();
    size_t max_streams = dag_context.initialize_concurrency;
    assert(max_streams > 0);

    // Interpreter test will not use columns in MockStorage
    if (context.isInterpreterTest() || !context.mockStorage()->exchangeExists(executor_id))
    {
        /// build with empty blocks.
        size_t stream_count = max_streams;
        if (fine_grained_stream_count > 0)
            stream_count = fine_grained_stream_count;
        for (size_t i = 0; i < stream_count; ++i)
            mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(exchange_receiver, context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
        for (const auto & col : mock_streams.back()->getHeader())
            schema.emplace_back(col.name, col.type);
    }
    else
    {
        /// build from user input blocks.
        if (fine_grained_stream_count > 0)
        {
            std::vector<ColumnsWithTypeAndName> columns_with_type_and_name_vector;
            columns_with_type_and_name_vector = context.mockStorage()->getFineGrainedExchangeColumnsVector(executor_id, fine_grained_stream_count);
            if (columns_with_type_and_name_vector.empty())
            {
                for (size_t i = 0; i < fine_grained_stream_count; ++i)
                    mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(exchange_receiver, context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
            }
            else
            {
                for (const auto & columns : columns_with_type_and_name_vector)
                    mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(columns, context.getSettingsRef().max_block_size));
            }
            for (const auto & col : mock_streams.back()->getHeader())
                schema.emplace_back(col.name, col.type);
        }
        else
        {
            auto [names_and_types, mock_exchange_streams] = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, log, executor_id);
            schema = std::move(names_and_types);
            mock_streams.insert(mock_streams.end(), mock_exchange_streams.begin(), mock_exchange_streams.end());
        }
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
    const tipb::ExchangeReceiver & exchange_receiver,
    size_t fine_grained_stream_count)
{
    auto [schema, mock_streams] = mockSchemaAndStreams(context, executor_id, log, exchange_receiver, fine_grained_stream_count);

    auto physical_mock_exchange_receiver = std::make_shared<PhysicalMockExchangeReceiver>(
        executor_id,
        schema,
        log->identifier(),
        Block(schema),
        mock_streams,
        static_cast<size_t>(exchange_receiver.encoded_task_meta_size()));
    return physical_mock_exchange_receiver;
}

void PhysicalMockExchangeReceiver::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    assert(pipeline.streams.empty());
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

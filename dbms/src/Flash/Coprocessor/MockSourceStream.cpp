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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/MockSourceStream.h>

namespace DB
{
std::pair<NamesAndTypes, std::vector<std::shared_ptr<MockTableScanBlockInputStream>>> mockSourceStreamForMpp(
    Context & context,
    size_t max_streams,
    DB::LoggerPtr log,
    const TiDBTableScan & table_scan)
{
    ColumnsWithTypeAndName columns_with_type_and_name = context.mockStorage()->getColumnsForMPPTableScan(
        table_scan,
        context.mockMPPServerInfo().partition_id,
        context.mockMPPServerInfo().partition_num);
    return cutStreams<MockTableScanBlockInputStream>(context, columns_with_type_and_name, max_streams, log);
}
size_t getMockSourceStreamConcurrency(size_t max_streams, size_t scan_concurrency_hint)
{
    if (scan_concurrency_hint == 0)
        return max_streams;
    return std::max(std::min(max_streams, scan_concurrency_hint), 1);
}
std::pair<NamesAndTypes, BlockInputStreams> mockSchemaAndStreamsForExchangeReceiver(
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
            stream_count = std::min(fine_grained_stream_count, max_streams);
        for (size_t i = 0; i < stream_count; ++i)
            mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(
                exchange_receiver,
                context.getSettingsRef().max_block_size,
                context.getSettingsRef().max_block_size / 10));
        for (const auto & col : mock_streams.back()->getHeader())
            schema.emplace_back(col.name, col.type);
    }
    else
    {
        /// build from user input blocks.
        if (fine_grained_stream_count > 0)
        {
            size_t output_stream_count = std::min(fine_grained_stream_count, max_streams);
            std::vector<ColumnsWithTypeAndName> columns_with_type_and_name_vector;
            columns_with_type_and_name_vector
                = context.mockStorage()->getFineGrainedExchangeColumnsVector(executor_id, fine_grained_stream_count);
            if (columns_with_type_and_name_vector.empty())
            {
                for (size_t i = 0; i < output_stream_count; ++i)
                    mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(
                        exchange_receiver,
                        context.getSettingsRef().max_block_size,
                        context.getSettingsRef().max_block_size / 10));
            }
            else
            {
                std::vector<std::vector<ColumnsWithTypeAndName>> columns_for_mock_exchange_receiver(
                    output_stream_count);
                for (size_t i = 0; i < columns_with_type_and_name_vector.size(); ++i)
                {
                    columns_for_mock_exchange_receiver[i % output_stream_count].push_back(
                        columns_with_type_and_name_vector[i]);
                }
                for (size_t i = 0; i < output_stream_count; ++i)
                    mock_streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(
                        columns_for_mock_exchange_receiver[i],
                        context.getSettingsRef().max_block_size));
            }
            for (const auto & col : mock_streams.back()->getHeader())
                schema.emplace_back(col.name, col.type);
        }
        else
        {
            auto [names_and_types, mock_exchange_streams]
                = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, log, executor_id);
            schema = std::move(names_and_types);
            mock_streams.insert(mock_streams.end(), mock_exchange_streams.begin(), mock_exchange_streams.end());
        }
    }

    assert(!schema.empty());
    assert(!mock_streams.empty());

    return {std::move(schema), std::move(mock_streams)};
}
} // namespace DB

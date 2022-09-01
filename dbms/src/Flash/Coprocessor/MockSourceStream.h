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
#pragma once

#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/MockExchangeReceiverInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>

namespace DB
{

template <typename SourceType>
std::pair<NamesAndTypes, std::vector<std::shared_ptr<SourceType>>> mockSourceStream(Context & context, size_t max_streams, DB::LoggerPtr log, String executor_id, Int64 table_id = 0)
{
    ColumnsWithTypeAndName columns_with_type_and_name;
    NamesAndTypes names_and_types;
    size_t rows = 0;
    std::vector<std::shared_ptr<SourceType>> mock_source_streams;
    if constexpr (std::is_same_v<SourceType, MockExchangeReceiverInputStream>)
        columns_with_type_and_name = context.mockStorage().getExchangeColumns(executor_id);
    else if (context.isMPPTest())
        columns_with_type_and_name = context.mockStorage().getColumnsForMPPTableScan(table_id, context.mockMPPServerInfo().partition_id, context.mockMPPServerInfo().partition_num);
    else
        columns_with_type_and_name = context.mockStorage().getColumns(table_id);

    for (const auto & col : columns_with_type_and_name)
    {
        if (rows == 0)
            rows = col.column->size();
        RUNTIME_ASSERT(rows == col.column->size(), log, "each column must has same size");
        names_and_types.push_back({col.name, col.type});
    }
    size_t row_for_each_stream = rows / max_streams;
    size_t rows_left = rows - row_for_each_stream * max_streams;
    size_t start = 0;
    for (size_t i = 0; i < max_streams; ++i)
    {
        ColumnsWithTypeAndName columns_for_stream;
        size_t row_for_current_stream = row_for_each_stream + (i < rows_left ? 1 : 0);
        for (const auto & column_with_type_and_name : columns_with_type_and_name)
        {
            columns_for_stream.push_back(
                ColumnWithTypeAndName(
                    column_with_type_and_name.column->cut(start, row_for_current_stream),
                    column_with_type_and_name.type,
                    column_with_type_and_name.name));
        }
        start += row_for_current_stream;
        mock_source_streams.emplace_back(std::make_shared<SourceType>(columns_for_stream, context.getSettingsRef().max_block_size));
    }
    RUNTIME_ASSERT(start == rows, log, "mock source streams' total size must same as user input");
    return {names_and_types, mock_source_streams};
}
} // namespace DB
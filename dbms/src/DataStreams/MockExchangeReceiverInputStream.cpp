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
#include <Flash/Coprocessor/ChunkCodec.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
MockExchangeReceiverInputStream::MockExchangeReceiverInputStream(
    const tipb::ExchangeReceiver & receiver,
    size_t max_block_size,
    size_t rows_)
    : max_block_size(max_block_size)
    , rows(rows_)
    , source_num(static_cast<size_t>(receiver.encoded_task_meta_size()))
{
    ColumnsWithTypeAndName columns;
    assert(receiver.field_types_size() > 0);
    for (int i = 0; i < receiver.field_types_size(); ++i)
    {
        columns.emplace_back(
            getDataTypeByColumnInfoForComputingLayer(TiDB::fieldTypeToColumnInfo(receiver.field_types(i))),
            fmt::format("exchange_receiver_{}", i));
    }
    columns_vector.push_back(std::move(columns));
}

void MockExchangeReceiverInputStream::initTotalRows()
{
    rows = 0;
    for (const auto & columns : columns_vector)
    {
        size_t current_rows = 0;
        for (const auto & elem : columns)
        {
            if (elem.column)
            {
                assert(current_rows == 0 || current_rows == elem.column->size());
                current_rows = elem.column->size();
            }
        }
        rows += current_rows;
    }
}

MockExchangeReceiverInputStream::MockExchangeReceiverInputStream(
    const ColumnsWithTypeAndName & columns,
    size_t max_block_size)
    : max_block_size(max_block_size)
{
    assert(!columns.empty());
    columns_vector.push_back(columns);
    initTotalRows();
}

MockExchangeReceiverInputStream::MockExchangeReceiverInputStream(
    const std::vector<ColumnsWithTypeAndName> & columns_vector_,
    size_t max_block_size)
    : columns_vector(columns_vector_)
    , max_block_size(max_block_size)
{
    assert(!columns_vector.empty() && !columns_vector[0].empty());
    initTotalRows();
}

ColumnPtr MockExchangeReceiverInputStream::makeColumn(ColumnWithTypeAndName elem) const
{
    auto column = elem.type->createColumn();
    size_t row_count = 0;
    size_t current_output_rows = output_rows;
    for (size_t i = output_index_in_current_columns;
         current_output_rows < rows && i < elem.column->size() && row_count < max_block_size;
         ++i, ++current_output_rows)
    {
        column->insert((*elem.column)[i]);
        ++row_count;
    }
    return column;
}

Block MockExchangeReceiverInputStream::readImpl()
{
    if (output_rows >= rows)
        return {};
    ColumnsWithTypeAndName output_columns;
    assert(columns_vector.size() > output_columns_index);
    for (const auto & elem : columns_vector[output_columns_index])
    {
        output_columns.push_back({makeColumn(elem), elem.type, elem.name, elem.column_id});
    }
    size_t return_rows = output_columns[0].column->size();
    output_rows += return_rows;
    output_index_in_current_columns += return_rows;
    if (output_index_in_current_columns == columns_vector[output_columns_index][0].column->size())
    {
        ++output_columns_index;
        output_index_in_current_columns = 0;
    }
    return Block(output_columns);
}
} // namespace DB

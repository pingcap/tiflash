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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <TiDB/Decode/TypeMapping.h>
#include <tipb/executor.pb.h>

namespace DB
{
/// Mock the receiver like table scan, can mock blocks according to the receiver's schema.
/// TODO: Mock the receiver process
class MockExchangeReceiverInputStream : public IProfilingBlockInputStream
{
public:
    MockExchangeReceiverInputStream(const tipb::ExchangeReceiver & receiver, size_t max_block_size, size_t rows_);
    MockExchangeReceiverInputStream(const ColumnsWithTypeAndName & columns, size_t max_block_size);
    MockExchangeReceiverInputStream(const std::vector<ColumnsWithTypeAndName> & columns_vector, size_t max_block_size);
    Block getHeader() const override { return Block(columns_vector[0]).cloneEmpty(); }
    String getName() const override { return "MockExchangeReceiver"; }
    size_t getSourceNum() const { return source_num; }
    std::vector<ColumnsWithTypeAndName> columns_vector;
    size_t output_rows = 0;
    size_t output_index_in_current_columns = 0;
    size_t output_columns_index = 0;
    size_t max_block_size;
    size_t rows = 0;
    size_t source_num = 0;

protected:
    Block readImpl() override;
    ColumnPtr makeColumn(ColumnWithTypeAndName elem) const;
    void initTotalRows();
};

} // namespace DB

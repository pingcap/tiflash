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

namespace DB
{
class MockTableScanBlockInputStream : public IProfilingBlockInputStream
{
public:
    MockTableScanBlockInputStream(ColumnsWithTypeAndName columns, size_t max_block_size);
    Block getHeader() const override
    {
        return Block(columns);
    }
    String getName() const override { return "MockTableScan"; }
    ColumnsWithTypeAndName columns;
    size_t output_index;
    size_t max_block_size;
    size_t rows;

protected:
    Block readImpl() override;
    ColumnPtr makeColumn(ColumnWithTypeAndName elem) const;
};

} // namespace DB

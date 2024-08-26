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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/ArrowColCodec.h>
#include <Flash/Coprocessor/TiDBChunk.h>
#include <Flash/Coprocessor/TiDBDecimal.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
TiDBChunk::TiDBChunk(const std::vector<tipb::FieldType> & field_types)
{
    for (const auto & type : field_types)
    {
        columns.emplace_back(getFieldLengthForArrowEncode(type.tp()));
    }
}

void TiDBChunk::buildDAGChunkFromBlock(
    const Block & block,
    const std::vector<tipb::FieldType> & field_types,
    size_t start_index,
    size_t end_index)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        flashColToArrowCol(columns[i], block.getByPosition(i), field_types[i], start_index, end_index);
    }
}

} // namespace DB

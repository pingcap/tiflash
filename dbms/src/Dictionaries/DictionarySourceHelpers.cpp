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

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <IO/WriteHelpers.h>


namespace DB
{

/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids)
{
    auto column = ColumnUInt64::create(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), "id"}};

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

/// For composite key
void formatKeys(
    const DictionaryStructure & dict_struct,
    BlockOutputStreamPtr & out,
    const Columns & key_columns,
    const std::vector<size_t> & requested_rows)
{
    Block block;
    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        auto filtered_column = source_column->cloneEmpty();
        filtered_column->reserve(requested_rows.size());

        for (size_t idx : requested_rows)
            filtered_column->insertFrom(*source_column, idx);

        block.insert({std::move(filtered_column), (*dict_struct.key)[i].type, toString(i)});
    }

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

} // namespace DB

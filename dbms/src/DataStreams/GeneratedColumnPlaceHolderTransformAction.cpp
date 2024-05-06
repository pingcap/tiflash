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

#include <DataStreams/GeneratedColumnPlaceHolderTransformAction.h>

namespace DB
{
GeneratedColumnPlaceHolderTransformAction::GeneratedColumnPlaceHolderTransformAction(
    const Block & header_,
    const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos_)
    : generated_column_infos(generated_column_infos_)
{
    header = header_;
    insertColumns(header, false);
}

Block GeneratedColumnPlaceHolderTransformAction::getHeader() const
{
    return header;
}

void GeneratedColumnPlaceHolderTransformAction::checkColumn() const
{
    RUNTIME_CHECK(!generated_column_infos.empty());
    // Validation check.
    for (size_t i = 1; i < generated_column_infos.size(); ++i)
    {
        RUNTIME_CHECK(std::get<0>(generated_column_infos[i]) > std::get<0>(generated_column_infos[i - 1]));
    }
}

void GeneratedColumnPlaceHolderTransformAction::insertColumns(Block & block, bool insert_data) const
{
    if (!block)
        return;

    for (const auto & ele : generated_column_infos)
    {
        const auto & col_index = std::get<0>(ele);
        const auto & col_name = std::get<1>(ele);
        const auto & data_type = std::get<2>(ele);
        ColumnPtr column = nullptr;
        if (insert_data)
            column = data_type->createColumnConstWithDefaultValue(block.rows());
        else
            column = data_type->createColumnConstWithDefaultValue(0);
        block.insert(col_index, ColumnWithTypeAndName{column, data_type, col_name});
    }
}

void GeneratedColumnPlaceHolderTransformAction::transform(Block & block)
{
    insertColumns(block, true);
}

} // namespace DB

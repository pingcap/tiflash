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

#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{

Block AddExtraTableIDColumnTransformAction::buildHeader(const Block & inner_header_, int extra_table_id_index)
{
    auto header = inner_header_.cloneEmpty();
    if (extra_table_id_index != InvalidColumnID)
    {
        const auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
        ColumnWithTypeAndName col{
            extra_table_id_col_define.type->createColumn(),
            extra_table_id_col_define.type,
            extra_table_id_col_define.name,
            extra_table_id_col_define.id,
            extra_table_id_col_define.default_value};
        header.insert(extra_table_id_index, col);
    }
    return header;
}

Block AddExtraTableIDColumnTransformAction::buildHeader(
    const DM::ColumnDefines & columns_to_read_,
    int extra_table_id_index)
{
    auto inner_header = toEmptyBlock(columns_to_read_);
    return buildHeader(inner_header, extra_table_id_index);
}

AddExtraTableIDColumnTransformAction::AddExtraTableIDColumnTransformAction(
    const Block & inner_header_,
    int extra_table_id_index_)
    : header(buildHeader(inner_header_, extra_table_id_index_))
    , extra_table_id_index(extra_table_id_index_)
{}

AddExtraTableIDColumnTransformAction::AddExtraTableIDColumnTransformAction(
    const DM::ColumnDefines & columns_to_read_,
    int extra_table_id_index_)
    : header(buildHeader(columns_to_read_, extra_table_id_index_))
    , extra_table_id_index(extra_table_id_index_)
{}

Block AddExtraTableIDColumnTransformAction::getHeader() const
{
    return header;
}

bool AddExtraTableIDColumnTransformAction::transform(Block & block, TableID physical_table_id)
{
    if (unlikely(!block))
        return true;

    if (extra_table_id_index != InvalidColumnID)
    {
        const auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
        ColumnWithTypeAndName col{
            {},
            extra_table_id_col_define.type,
            extra_table_id_col_define.name,
            extra_table_id_col_define.id};
        size_t row_number = block.rows();
        auto col_data = col.type->createColumnConst(row_number, Field(physical_table_id));
        col.column = std::move(col_data);
        block.insert(extra_table_id_index, std::move(col));
    }

    total_rows += block.rows();

    return true;
}

} // namespace DB

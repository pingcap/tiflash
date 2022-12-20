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

#include <DataStreams/SegmentReadTransformAction.h>

namespace DB
{
Block SegmentReadTransformAction::getHeader() const
{
    return header;
}

bool SegmentReadTransformAction::transform(Block & block)
{
    if (extra_table_id_index != InvalidColumnID)
    {
        const auto & extra_table_id_col_define = DM::getExtraTableIDColumnDefine();
        ColumnWithTypeAndName col{{}, extra_table_id_col_define.type, extra_table_id_col_define.name, extra_table_id_col_define.id};
        size_t row_number = block.rows();
        auto col_data = col.type->createColumnConst(row_number, Field(physical_table_id));
        col.column = std::move(col_data);
        block.insert(extra_table_id_index, std::move(col));
    }
    if (!block.rows())
    {
        return false;
    }
    else
    {
        total_rows += block.rows();
        return true;
    }
}
} // namespace DB

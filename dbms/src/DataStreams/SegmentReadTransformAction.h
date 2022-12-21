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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
struct SegmentReadTransformAction
{
public:
    SegmentReadTransformAction(
        const Block & header_,
        int extra_table_id_index_,
        TableID physical_table_id_)
        : header(header_)
        , extra_table_id_index(extra_table_id_index_)
        , physical_table_id(physical_table_id_)
    {
    }
    bool transform(Block & block);
    Block getHeader() const;
    size_t totalRows() const
    {
        return total_rows;
    }

private:
    Block header;
    // position of the ExtraPhysTblID column in column_names parameter in the StorageDeltaMerge::read function.
    const int extra_table_id_index;
    const TableID physical_table_id;
    size_t total_rows = 0;
};
} // namespace DB

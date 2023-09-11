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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
struct AddExtraTableIDColumnTransformAction
{
public:
    static Block buildHeader(const Block & inner_header_, int extra_table_id_index_);

    static Block buildHeader(const DM::ColumnDefines & columns_to_read_, int extra_table_id_index_);

    AddExtraTableIDColumnTransformAction(const Block & inner_header_, int extra_table_id_index_);

    AddExtraTableIDColumnTransformAction(const DM::ColumnDefines & columns_to_read_, int extra_table_id_index_);

    bool transform(Block & block, TableID physical_table_id);

    Block getHeader() const;

    size_t totalRows() const { return total_rows; }

private:
    Block header;
    // position of the ExtraPhysTblID column in column_names parameter in the StorageDeltaMerge::read function.
    const int extra_table_id_index;

    size_t total_rows = 0;
};

} // namespace DB

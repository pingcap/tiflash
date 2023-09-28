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

#include <Core/Block.h>
#include <Storages/KVStore/Types.h>

#include <unordered_map>

namespace DB
{

// A transform action that pickup columns in input block
// by the column ids defined by `out_header`. The column
// names in returned block is the same as `out_header`.
struct SelectionByColumnIdTransformAction
{
public:
    explicit SelectionByColumnIdTransformAction(const Block & in_header_, const Block & out_header_);

    Block transform(const Block & in_block);

    Block filterAndTransform(const Block & in_block, const IColumn::Filter & filter, ssize_t result_size_hint);

    Block getHeader() const { return out_header; }

private:
    Block out_header;
    // The ColumnID -> offset mapping in input blocks
    std::unordered_map<ColumnID, size_t> col_offset_by_id;
};
} // namespace DB

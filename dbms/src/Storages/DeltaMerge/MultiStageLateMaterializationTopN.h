// Copyright 2026 PingCAP, Inc.
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

#include <Columns/IColumn.h>
#include <Common/Decimal.h>
#include <Core/Block.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>

#include <array>
#include <memory>
#include <queue>
#include <variant>
#include <vector>

namespace DB::DM
{

inline constexpr UInt64 multi_stage_late_materialization_topn_max_topk = 4096;
inline constexpr size_t multi_stage_late_materialization_topn_max_order_by_columns = 4;

struct MultiStageLateMaterializationTopNOrderByColumn
{
    ColumnID column_id;
    int direction;
};

struct MultiStageLateMaterializationTopNDescription
{
    UInt64 topk = 0;
    std::vector<MultiStageLateMaterializationTopNOrderByColumn> order_by_columns;
};

using MultiStageLateMaterializationTopNDescriptionPtr = std::shared_ptr<MultiStageLateMaterializationTopNDescription>;

enum class SortKeyKind
{
    Int64,
    UInt64,
    Float32,
    Float64,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Date,
    DateTime,
};

using SortKeyValue = std::variant<Int64, UInt64, Float32, Float64, Decimal32, Decimal64, Decimal128, Decimal256>;

struct SortKeyField
{
    bool is_null = false;
    SortKeyKind kind = SortKeyKind::Int64;
    SortKeyValue value = Int64{};
};

struct OwnedSortKey
{
    std::array<SortKeyField, multi_stage_late_materialization_topn_max_order_by_columns> fields;
    size_t size = 0;
};

struct RunningLocalTopNUpdateResult
{
    IColumn::Filter filter;
    size_t passed_count = 0;
};

class RunningLocalTopN
{
public:
    RunningLocalTopN(
        const MultiStageLateMaterializationTopNDescription & description,
        const ColumnDefines & stage1_columns);

    RunningLocalTopNUpdateResult update(
        const Block & stage1_block,
        const IColumn::Filter * residual_filter,
        size_t residual_passed_rows);

    size_t heapSize() const { return heap.size(); }

private:
    struct SortKeyColumnDesc
    {
        using ExtractFieldFn = void (*)(const IColumn & column, size_t row, SortKeyField & field);

        ColumnID column_id = EmptyColumnID;
        size_t column_pos = 0;
        SortKeyKind kind = SortKeyKind::Int64;
        int direction = 1;
        ExtractFieldFn extract_field = nullptr;
    };

    struct HeapEntry
    {
        OwnedSortKey key;
        UInt64 block_sequence = 0;
        UInt32 row_index_in_stage1_block = 0;
    };

    struct HeapComparator
    {
        const RunningLocalTopN * owner = nullptr;

        bool operator()(const HeapEntry & lhs, const HeapEntry & rhs) const;
    };

    using Heap = std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapComparator>;

    int compareOwnedKeys(const OwnedSortKey & lhs, const OwnedSortKey & rhs) const;
    int compareRowWithOwnedKey(const std::vector<const IColumn *> & sort_columns, size_t row, const OwnedSortKey & rhs)
        const;
    OwnedSortKey materializeOwnedKey(const std::vector<const IColumn *> & sort_columns, size_t row) const;

private:
    UInt64 topk;
    std::vector<SortKeyColumnDesc> sort_key_columns;
    UInt64 current_block_sequence = 0;
    Heap heap;
};

} // namespace DB::DM

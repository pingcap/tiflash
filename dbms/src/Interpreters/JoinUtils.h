// Copyright 2023 PingCAP, Ltd.
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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
/// Do I need to use the hash table maps_*_full, in which we remember whether the row was joined and fill left columns if not joined
inline bool getFullness(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::RightOuter || kind == ASTTableJoin::Kind::Cross_RightOuter || kind == ASTTableJoin::Kind::Full;
}
/// For semi and anti join: A semi/anti join B, that uses A as build table
inline bool isRightSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::RightSemi || kind == ASTTableJoin::Kind::RightAnti;
}
inline bool isLeftOuterJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::LeftOuter || kind == ASTTableJoin::Kind::Cross_LeftOuter;
}
inline bool isRightOuterJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::RightOuter || kind == ASTTableJoin::Kind::Cross_RightOuter;
}
inline bool isInnerJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross;
}
inline bool isAntiJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Anti || kind == ASTTableJoin::Kind::Cross_Anti;
}
inline bool isCrossJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Cross || kind == ASTTableJoin::Kind::Cross_LeftOuter
        || kind == ASTTableJoin::Kind::Cross_RightOuter || kind == ASTTableJoin::Kind::Cross_Anti
        || kind == ASTTableJoin::Kind::Cross_LeftOuterSemi || kind == ASTTableJoin::Kind::Cross_LeftOuterAnti;
}
/// (cartesian/null-aware) (anti) left outer semi join.
inline bool isLeftOuterSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::LeftOuterSemi || kind == ASTTableJoin::Kind::LeftOuterAnti
        || kind == ASTTableJoin::Kind::Cross_LeftOuterSemi || kind == ASTTableJoin::Kind::Cross_LeftOuterAnti
        || kind == ASTTableJoin::Kind::NullAware_LeftOuterSemi || kind == ASTTableJoin::Kind::NullAware_LeftOuterAnti;
}
inline bool isNullAwareSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::NullAware_Anti || kind == ASTTableJoin::Kind::NullAware_LeftOuterAnti
        || kind == ASTTableJoin::Kind::NullAware_LeftOuterSemi;
}
inline bool needRecordNotInsertRows(ASTTableJoin::Kind kind)
{
    return getFullness(kind) || (kind == ASTTableJoin::Kind::RightAnti) || isNullAwareSemiFamily(kind);
}
inline bool needScanHashMapAfterProbe(ASTTableJoin::Kind kind)
{
    return getFullness(kind) || isRightSemiFamily(kind);
}

inline bool isNecessaryKindToUseRowFlaggedHashMap(ASTTableJoin::Kind kind)
{
    return isRightSemiFamily(kind) || kind == ASTTableJoin::Kind::RightOuter;
}

inline bool useRowFlaggedHashMap(ASTTableJoin::Kind kind, bool has_other_condition)
{
    return has_other_condition && isNecessaryKindToUseRowFlaggedHashMap(kind);
}

/// incremental probe means a probe row can be probed by each right block independently
inline bool supportIncrementalProbeForCrossJoin(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness)
{
    if (isCrossJoin(kind))
    {
        if (kind == ASTTableJoin::Kind::Cross_LeftOuter || kind == ASTTableJoin::Kind::Cross_RightOuter)
        {
            assert(strictness == ASTTableJoin::Strictness::All);
            return true;
        }
        if (kind == ASTTableJoin::Kind::Cross && strictness == ASTTableJoin::Strictness::All)
            return true;
        /// cross any join is semi join, don't support incremental probe
    }
    return false;
}

bool mayProbeSideExpandedAfterJoin(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness);

enum class CrossProbeMode
{
    NORMAL,
    NO_COPY_RIGHT_BLOCK,
};

struct ProbeProcessInfo
{
    Block block;
    size_t partition_index;
    UInt64 max_block_size;
    UInt64 min_result_block_size;
    size_t start_row;
    size_t end_row;
    bool all_rows_joined_finish;

    /// these should be inited before probe each block
    bool prepare_for_probe_done = false;
    ColumnPtr null_map_holder = nullptr;
    ConstNullMapPtr null_map = nullptr;
    /// Used with ANY INNER JOIN
    std::unique_ptr<IColumn::Filter> filter = nullptr;
    /// Used with ALL ... JOIN
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate = nullptr;

    /// for hash probe
    Columns materialized_columns;
    ColumnRawPtrs key_columns;

    /// for cross probe
    Block result_block_schema;
    std::vector<size_t> right_column_index;
    size_t right_rows_to_be_added_when_matched = 0;
    /// the following 4 fields are used for NO_COPY_RIGHT_BLOCK probe
    bool use_incremental_probe = false;
    size_t next_right_block_index = 0;
    size_t right_block_size = 0;
    size_t row_num_filtered_by_left_condition = 0;

    explicit ProbeProcessInfo(UInt64 max_block_size_)
        : partition_index(0)
        , max_block_size(max_block_size_)
        , min_result_block_size((max_block_size + 1) / 2)
        , start_row(0)
        , end_row(0)
        , all_rows_joined_finish(true){};

    void resetBlock(Block && block_, size_t partition_index_ = 0);
    template <bool cross_join>
    void updateStartRow()
    {
        if constexpr (cross_join)
        {
            if (use_incremental_probe)
            {
                if (next_right_block_index < right_block_size)
                    return;
                next_right_block_index = 0;
            }
        }
        assert(start_row <= end_row);
        start_row = end_row;
        if (filter != nullptr)
            filter->resize(block.rows());
        if (offsets_to_replicate != nullptr)
            offsets_to_replicate->resize(block.rows());
    }

    template <bool cross_join>
    void updateEndRow(size_t next_row_to_probe)
    {
        if constexpr (cross_join)
        {
            if (use_incremental_probe && next_right_block_index < right_block_size)
            {
                /// current probe is not finished, just return
                return;
            }
            end_row = next_row_to_probe;
            all_rows_joined_finish = row_num_filtered_by_left_condition == 0 && end_row == block.rows();
        }
        else
        {
            end_row = next_row_to_probe;
            all_rows_joined_finish = end_row == block.rows();
        }
    }

    void prepareForHashProbe(const Names & key_names, const String & filter_column, ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness);
    void prepareForCrossProbe(
        const String & filter_column,
        ASTTableJoin::Kind kind,
        ASTTableJoin::Strictness strictness,
        const Block & sample_block_with_columns_to_add,
        size_t right_rows_to_be_added_when_matched,
        bool use_incremental_probe,
        size_t right_block_size);
};
struct JoinBuildInfo
{
    bool enable_fine_grained_shuffle;
    size_t fine_grained_shuffle_count;
    bool enable_spill;
    bool is_spilled;
    size_t build_concurrency;
    size_t restore_round;
    bool needVirtualDispatchForProbeBlock() const
    {
        return enable_fine_grained_shuffle || (enable_spill && !is_spilled);
    }
};
void computeDispatchHash(size_t rows,
                         const ColumnRawPtrs & key_columns,
                         const TiDB::TiDBCollators & collators,
                         std::vector<String> & partition_key_containers,
                         size_t join_restore_round,
                         WeakHash32 & hash);

template <int>
struct PointerTypeColumnHelper;

template <>
struct PointerTypeColumnHelper<4>
{
    using DataType = DataTypeInt32;
    using ColumnType = ColumnVector<Int32>;
    using ArrayType = PaddedPODArray<Int32>;
};

template <>
struct PointerTypeColumnHelper<8>
{
    using DataType = DataTypeInt64;
    using ColumnType = ColumnVector<Int64>;
    using ArrayType = PaddedPODArray<Int64>;
};

ColumnRawPtrs extractAndMaterializeKeyColumns(const Block & block, Columns & materialized_columns, const Strings & key_columns_names);
void recordFilteredRows(const Block & block, const String & filter_column, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map);
} // namespace DB

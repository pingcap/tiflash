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
    return kind == ASTTableJoin::Kind::RightOuter || kind == ASTTableJoin::Kind::Cross_RightOuter
        || kind == ASTTableJoin::Kind::Full;
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
inline bool isSemiJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Semi || kind == ASTTableJoin::Kind::Cross_Semi;
}
inline bool isAntiJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Anti || kind == ASTTableJoin::Kind::Cross_Anti;
}
inline bool isCrossJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Cross || kind == ASTTableJoin::Kind::Cross_LeftOuter
        || kind == ASTTableJoin::Kind::Cross_RightOuter || kind == ASTTableJoin::Kind::Cross_Semi
        || kind == ASTTableJoin::Kind::Cross_Anti || kind == ASTTableJoin::Kind::Cross_LeftOuterSemi
        || kind == ASTTableJoin::Kind::Cross_LeftOuterAnti;
}
/// (anti) semi join.
inline bool isSemiFamily(ASTTableJoin::Kind kind)
{
    return isSemiJoin(kind) || isAntiJoin(kind);
}
/// For semi and anti join: A semi/anti join B, that uses A as build table
inline bool isRightSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::RightSemi || kind == ASTTableJoin::Kind::RightAnti;
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

bool mayProbeSideExpandedAfterJoin(ASTTableJoin::Kind kind);

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
void computeDispatchHash(
    size_t rows,
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

ColumnRawPtrs extractAndMaterializeKeyColumns(
    const Block & block,
    Columns & materialized_columns,
    const Strings & key_columns_names);
void recordFilteredRows(
    const Block & block,
    const String & filter_column,
    ColumnPtr & null_map_holder,
    ConstNullMapPtr & null_map);

std::pair<const ColumnUInt8::Container *, ConstNullMapPtr> getDataAndNullMapVectorFromFilterColumn(
    ColumnPtr & filter_column);

void mergeNullAndFilterResult(
    Block & block,
    ColumnVector<UInt8>::Container & filter_column,
    const String & filter_column_name,
    bool null_as_true);

} // namespace DB

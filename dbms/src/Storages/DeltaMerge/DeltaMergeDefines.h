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

#include <Common/Allocator.h>
#include <Common/ArenaWithFreeLists.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>

#include <limits>
#include <memory>
#include <optional>

namespace TiDB
{
struct TableInfo;
} // namespace TiDB

namespace DB::DM
{
/// S: Scale factor of delta tree node.
/// M: Capacity factor of leaf node.
/// F: Capacity factor of Intern node.
/// For example, the max capacity of leaf node would be: M * S.
static constexpr size_t DT_S = 3;
static constexpr size_t DT_M = 55;
static constexpr size_t DT_F = 20;

template <class ValueSpace, size_t M, size_t F, size_t S = 3, typename TAllocator = Allocator<false>>
class DeltaTree;

template <size_t M, size_t F, size_t S>
class DTEntryIterator;

template <size_t M, size_t F, size_t S, typename TAllocator = Allocator<false>>
class DTEntriesCopy;

struct EmptyValueSpace
{
    void removeFromInsert(UInt64) {}
};

using EntryIterator = DTEntryIterator<DT_M, DT_F, DT_S>;
using DefaultDeltaTree = DeltaTree<EmptyValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using DeltaTreePtr = std::shared_ptr<DefaultDeltaTree>;
using BlockPtr = std::shared_ptr<Block>;
using ColId = DB::ColumnID;
using ColIds = std::vector<ColId>;
using Handle = DB::HandleID;
using RowsAndBytes = std::pair<size_t, size_t>;
using OptionTableInfoConstRef = std::optional<std::reference_wrapper<const TiDB::TableInfo>>;

struct ColumnDefine
{
    ColId id;
    String name;
    DataTypePtr type;
    Field default_value;

    explicit ColumnDefine(ColId id_ = 0, String name_ = "", DataTypePtr type_ = nullptr, Field default_value_ = Field{})
        : id(id_)
        , name(std::move(name_))
        , type(std::move(type_))
        , default_value(std::move(default_value_))
    {}
};

inline auto format_as(const ColumnDefine & cd)
{
    return fmt::format("{}/{}", cd.id, cd.type->getName());
}

inline static constexpr UInt64 INITIAL_EPOCH = 0;
inline static constexpr bool DM_RUN_CHECK = true;

ALWAYS_INLINE const ColumnDefine & getExtraIntHandleColumnDefine();
ALWAYS_INLINE const ColumnDefine & getExtraStringHandleColumnDefine();
ALWAYS_INLINE const ColumnDefine & getExtraHandleColumnDefine(bool is_common_handle);
ALWAYS_INLINE const ColumnDefine & getVersionColumnDefine();
ALWAYS_INLINE const ColumnDefine & getTagColumnDefine();
ALWAYS_INLINE const ColumnDefine & getExtraTableIDColumnDefine();

static_assert(
    static_cast<Int64>(static_cast<UInt64>(std::numeric_limits<Int64>::min())) == std::numeric_limits<Int64>::min(),
    "Unsupported compiler!");
static_assert(
    static_cast<Int64>(static_cast<UInt64>(std::numeric_limits<Int64>::max())) == std::numeric_limits<Int64>::max(),
    "Unsupported compiler!");

struct Attr
{
    String col_name;
    ColId col_id;
    DataTypePtr type;
};
using Attrs = std::vector<Attr>;
} // namespace DB::DM

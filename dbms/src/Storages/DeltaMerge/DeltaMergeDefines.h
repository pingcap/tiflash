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
#include <Common/EventRecorder.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <TiDB/Schema/VectorIndex.h>

#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

namespace TiDB
{
struct TableInfo;
} // namespace TiDB

namespace DB
{
namespace DM
{
/// S: Scale factor of delta tree node.
/// M: Capacity factor of leaf node.
/// F: Capacity factor of Intern node.
/// For example, the max capacity of leaf node would be: M * S.
static constexpr size_t DT_S = 3;
static constexpr size_t DT_M = 55;
static constexpr size_t DT_F = 20;

using Ids = std::vector<UInt64>;

template <class ValueSpace, size_t M, size_t F, size_t S = 3, typename TAllocator = Allocator<false>>
class DeltaTree;

template <size_t M, size_t F, size_t S>
class DTEntryIterator;

template <size_t M, size_t F, size_t S, typename TAllocator = Allocator<false>>
class DTEntriesCopy;

struct RefTuple;

struct EmptyValueSpace
{
    void removeFromInsert(UInt64) {}
};

using EntryIterator = DTEntryIterator<DT_M, DT_F, DT_S>;
using DefaultDeltaTree = DeltaTree<EmptyValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using DeltaTreePtr = std::shared_ptr<DefaultDeltaTree>;
using BlockPtr = std::shared_ptr<Block>;

using RowId = UInt64;
using ColId = DB::ColumnID;
using Handle = DB::HandleID;

using ColIds = std::vector<ColId>;
using HandlePair = std::pair<Handle, Handle>;

using RowsAndBytes = std::pair<size_t, size_t>;

using OptionTableInfoConstRef = std::optional<std::reference_wrapper<const TiDB::TableInfo>>;

struct ColumnDefine
{
    ColId id;
    String name;
    DataTypePtr type;
    Field default_value;

    /// Note: ColumnDefine is used in both Write path and Read path.
    /// In the read path, vector_index is usually not available. Use AnnQueryInfo for
    /// read related vector index information.
    TiDB::VectorIndexInfoPtr vector_index;

    explicit ColumnDefine(
        ColId id_ = 0,
        String name_ = "",
        DataTypePtr type_ = nullptr,
        Field default_value_ = Field{},
        TiDB::VectorIndexInfoPtr vector_index_ = nullptr)
        : id(id_)
        , name(std::move(name_))
        , type(std::move(type_))
        , default_value(std::move(default_value_))
        , vector_index(vector_index_)
    {}
};

using ColumnDefines = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;
using ColumnDefineMap = std::unordered_map<ColId, ColumnDefine>;

using ColumnMap = std::unordered_map<ColId, ColumnPtr>;
using MutableColumnMap = std::unordered_map<ColId, MutableColumnPtr>;
using LockGuard = std::lock_guard<std::mutex>;

inline static const UInt64 INITIAL_EPOCH = 0;

// TODO maybe we should use those variables instead of macros?
#define EXTRA_HANDLE_COLUMN_NAME ::DB::MutableSupport::tidb_pk_column_name
#define VERSION_COLUMN_NAME ::DB::MutableSupport::version_column_name
#define TAG_COLUMN_NAME ::DB::MutableSupport::delmark_column_name
#define EXTRA_TABLE_ID_COLUMN_NAME ::DB::MutableSupport::extra_table_id_column_name

#define EXTRA_HANDLE_COLUMN_ID ::DB::TiDBPkColumnID
#define VERSION_COLUMN_ID ::DB::VersionColumnID
#define TAG_COLUMN_ID ::DB::DelMarkColumnID
#define EXTRA_TABLE_ID_COLUMN_ID ::DB::ExtraTableIDColumnID

#define EXTRA_HANDLE_COLUMN_INT_TYPE ::DB::MutableSupport::tidb_pk_column_int_type
#define EXTRA_HANDLE_COLUMN_STRING_TYPE ::DB::MutableSupport::tidb_pk_column_string_type
#define VERSION_COLUMN_TYPE ::DB::MutableSupport::version_column_type
#define TAG_COLUMN_TYPE ::DB::MutableSupport::delmark_column_type
#define EXTRA_TABLE_ID_COLUMN_TYPE ::DB::MutableSupport::extra_table_id_column_type

inline const ColumnDefine & getExtraIntHandleColumnDefine()
{
    static ColumnDefine EXTRA_HANDLE_COLUMN_DEFINE_{
        EXTRA_HANDLE_COLUMN_ID,
        EXTRA_HANDLE_COLUMN_NAME,
        EXTRA_HANDLE_COLUMN_INT_TYPE};
    return EXTRA_HANDLE_COLUMN_DEFINE_;
}
inline const ColumnDefine & getExtraStringHandleColumnDefine()
{
    static ColumnDefine EXTRA_HANDLE_COLUMN_DEFINE_{
        EXTRA_HANDLE_COLUMN_ID,
        EXTRA_HANDLE_COLUMN_NAME,
        EXTRA_HANDLE_COLUMN_STRING_TYPE};
    return EXTRA_HANDLE_COLUMN_DEFINE_;
}
inline const ColumnDefine & getExtraHandleColumnDefine(bool is_common_handle)
{
    if (is_common_handle)
        return getExtraStringHandleColumnDefine();
    return getExtraIntHandleColumnDefine();
}
inline const ColumnDefine & getVersionColumnDefine()
{
    static ColumnDefine VERSION_COLUMN_DEFINE_{VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE};
    return VERSION_COLUMN_DEFINE_;
}
inline const ColumnDefine & getTagColumnDefine()
{
    static ColumnDefine TAG_COLUMN_DEFINE_{TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE};
    return TAG_COLUMN_DEFINE_;
}
inline const ColumnDefine & getExtraTableIDColumnDefine()
{
    static ColumnDefine EXTRA_TABLE_ID_COLUMN_DEFINE_{
        EXTRA_TABLE_ID_COLUMN_ID,
        EXTRA_TABLE_ID_COLUMN_NAME,
        EXTRA_TABLE_ID_COLUMN_TYPE};
    return EXTRA_TABLE_ID_COLUMN_DEFINE_;
}

static_assert(
    static_cast<Int64>(static_cast<UInt64>(std::numeric_limits<Int64>::min())) == std::numeric_limits<Int64>::min(),
    "Unsupported compiler!");
static_assert(
    static_cast<Int64>(static_cast<UInt64>(std::numeric_limits<Int64>::max())) == std::numeric_limits<Int64>::max(),
    "Unsupported compiler!");

static constexpr bool DM_RUN_CHECK = true;

} // namespace DM
} // namespace DB

template <>
struct fmt::formatter<DB::DM::ColumnDefine>
{
    template <typename FormatContext>
    auto format(const DB::DM::ColumnDefine & cd, FormatContext & ctx) const -> decltype(ctx.out())
    {
        // Use '/' as separators because column names often have '_'.
        return format_to(ctx.out(), "{}/{}/{}", cd.id, cd.name, cd.type->getName());
    }
};

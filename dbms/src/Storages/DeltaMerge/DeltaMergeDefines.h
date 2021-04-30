#pragma once

#include <Common/Allocator.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/EventRecorder.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Types.h>
#include <Storages/FormatVersion.h>

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

using EntryIterator    = DTEntryIterator<DT_M, DT_F, DT_S>;
using DefaultDeltaTree = DeltaTree<EmptyValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using DeltaTreePtr     = std::shared_ptr<DefaultDeltaTree>;
using BlockPtr         = std::shared_ptr<Block>;

using RowId  = UInt64;
using ColId  = DB::ColumnID;
using Handle = DB::HandleID;

using ColIds     = std::vector<ColId>;
using HandlePair = std::pair<Handle, Handle>;

using RowsAndBytes = std::pair<size_t, size_t>;

using OptionTableInfoConstRef = std::optional<std::reference_wrapper<const TiDB::TableInfo>>;

struct ColumnDefine
{
    ColId       id;
    String      name;
    DataTypePtr type;
    Field       default_value;

    explicit ColumnDefine(ColId id_ = 0, String name_ = "", DataTypePtr type_ = nullptr, Field default_value_ = Field{})
        : id(id_), name(std::move(name_)), type(std::move(type_)), default_value(std::move(default_value_))
    {
    }
};

using ColumnDefines    = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;
using ColumnDefineMap  = std::unordered_map<ColId, ColumnDefine>;

using ColumnMap        = std::unordered_map<ColId, ColumnPtr>;
using MutableColumnMap = std::unordered_map<ColId, MutableColumnPtr>;
using LockGuard        = std::lock_guard<std::mutex>;

inline static const UInt64 INITIAL_EPOCH = 0;

// TODO maybe we should use those variables instead of macros?
#define EXTRA_HANDLE_COLUMN_NAME ::DB::MutableSupport::tidb_pk_column_name
#define VERSION_COLUMN_NAME ::DB::MutableSupport::version_column_name
#define TAG_COLUMN_NAME ::DB::MutableSupport::delmark_column_name

#define EXTRA_HANDLE_COLUMN_ID ::DB::TiDBPkColumnID
#define VERSION_COLUMN_ID ::DB::VersionColumnID
#define TAG_COLUMN_ID ::DB::DelMarkColumnID

#define EXTRA_HANDLE_COLUMN_INT_TYPE ::DB::MutableSupport::tidb_pk_column_int_type
#define EXTRA_HANDLE_COLUMN_STRING_TYPE ::DB::MutableSupport::tidb_pk_column_string_type
#define VERSION_COLUMN_TYPE ::DB::MutableSupport::version_column_type
#define TAG_COLUMN_TYPE ::DB::MutableSupport::delmark_column_type

inline const ColumnDefine & getExtraIntHandleColumnDefine()
{
    static ColumnDefine EXTRA_HANDLE_COLUMN_DEFINE_{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
    return EXTRA_HANDLE_COLUMN_DEFINE_;
}
inline const ColumnDefine & getExtraStringHandleColumnDefine()
{
    static ColumnDefine EXTRA_HANDLE_COLUMN_DEFINE_{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_STRING_TYPE};
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

static constexpr UInt64 MIN_UINT64 = std::numeric_limits<UInt64>::min();
static constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

static constexpr Int64 MIN_INT64 = std::numeric_limits<Int64>::min();
static constexpr Int64 MAX_INT64 = std::numeric_limits<Int64>::max();

static constexpr Handle N_INF_HANDLE = MIN_INT64; // Used in range, indicating negative infinity.
static constexpr Handle P_INF_HANDLE = MAX_INT64; // Used in range, indicating positive infinity.

static_assert(static_cast<Int64>(static_cast<UInt64>(MIN_INT64)) == MIN_INT64, "Unsupported compiler!");
static_assert(static_cast<Int64>(static_cast<UInt64>(MAX_INT64)) == MAX_INT64, "Unsupported compiler!");

static constexpr bool DM_RUN_CHECK = true;

} // namespace DM
} // namespace DB

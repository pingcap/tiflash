#pragma once

#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

#include <Common/Allocator.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/EventRecorder.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Types.h>

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
static constexpr size_t DT_F = 13;

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
    void   removeFromInsert(UInt64) {}
    void   removeFromModify(UInt64, size_t) {}
    UInt64 withModify(UInt64, const EmptyValueSpace &, const RefTuple &) { throw Exception("Unsupported operation"); }
};

using EntryIterator    = DTEntryIterator<DT_M, DT_F, DT_S>;
using DefaultDeltaTree = DeltaTree<EmptyValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using DeltaTreePtr     = std::shared_ptr<DefaultDeltaTree>;
using DeltaIndex       = DTEntriesCopy<DT_M, DT_F, DT_S>;
using DeltaIndexPtr    = std::shared_ptr<DeltaIndex>;

using Handle = Int64;
using RowId  = UInt64;
using ColId  = DB::ColumnID;

using ColIds     = std::vector<ColId>;
using HandlePair = std::pair<Handle, Handle>;

using OptionTableInfoConstRef = std::optional<std::reference_wrapper<const TiDB::TableInfo>>;

struct ColumnDefine
{
    ColId       id;
    String      name;
    DataTypePtr type;
    String      default_value;

    explicit ColumnDefine(ColId id_ = 0, String name_ = "", DataTypePtr type_ = nullptr)
        : id(id_), name(std::move(name_)), type(std::move(type_))
    {
    }
};
using ColumnDefines   = std::vector<ColumnDefine>;
using ColumnDefineMap = std::unordered_map<ColId, ColumnDefine>;

using ColumnMap        = std::unordered_map<ColId, ColumnPtr>;
using MutableColumnMap = std::unordered_map<ColId, MutableColumnPtr>;
using LockGuard        = std::lock_guard<std::mutex>;

static const UInt64 INITIAL_EPOCH = 5; // Following TiDB, and I have no idea why 5 is chosen.

// TODO maybe we should use those variables instead of macros?
#define EXTRA_HANDLE_COLUMN_NAME ::DB::MutableSupport::tidb_pk_column_name
#define VERSION_COLUMN_NAME ::DB::MutableSupport::version_column_name
#define TAG_COLUMN_NAME ::DB::MutableSupport::delmark_column_name

#define EXTRA_HANDLE_COLUMN_ID ::DB::TiDBPkColumnID
#define VERSION_COLUMN_ID ::DB::VersionColumnID
#define TAG_COLUMN_ID ::DB::DelMarkColumnID

#define EXTRA_HANDLE_COLUMN_TYPE ::DB::MutableSupport::tidb_pk_column_type
#define VERSION_COLUMN_TYPE ::DB::MutableSupport::version_column_type
#define TAG_COLUMN_TYPE ::DB::MutableSupport::delmark_column_type

inline const ColumnDefine & getExtraHandleColumnDefine()
{
    static ColumnDefine EXTRA_HANDLE_COLUMN_DEFINE_{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_TYPE};
    return EXTRA_HANDLE_COLUMN_DEFINE_;
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

static constexpr UInt64 DEL_RANGE_POS_MARK = (1ULL << 63);

} // namespace DM
} // namespace DB
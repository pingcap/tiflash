#pragma once

#include <limits>
#include <memory>

#include <Common/Allocator.h>
#include <Common/ArenaWithFreeLists.h>
#include <DataTypes/DataTypeFactory.h>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <type_traits>

namespace DB
{
/// S: Scale factor of delta tree node.
/// M: Capacity factor of leaf node.
/// F: Capacity factor of Intern node.
/// For example, the max capacity of leaf node would be: M * S.
static constexpr size_t DT_S = 3;
static constexpr size_t DT_M = 55;
static constexpr size_t DT_F = 13;

static constexpr UInt64 INVALID_ID = 0;

using Ids = std::vector<UInt64>;

template <class ValueSpace, size_t M, size_t F, size_t S = 3, typename TAllocator = Allocator<false>>
class DeltaTree;

template <size_t M, size_t F, size_t S>
class DTEntryIterator;

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

using Handle = Int64;
using RowId  = UInt64;
using ColId  = Int64;

using ColIds = std::vector<ColId>;

struct ColumnDefine
{
    ColId       id;
    String      name;
    DataTypePtr type;
};
using ColumnDefines   = std::vector<ColumnDefine>;
using ColumnDefineMap = std::unordered_map<ColId, ColumnDefine>;

using ColumnMap        = std::unordered_map<ColId, ColumnPtr>;
using MutableColumnMap = std::unordered_map<ColId, MutableColumnPtr>;
using LockGuard        = std::lock_guard<std::mutex>;

static const UInt64 INITIAL_EPOCH = 5; // Following TiDB, and I have no idea why 5 is chosen.

static const String EXTRA_HANDLE_COLUMN_NAME = "_extra_handle_";
static const String VERSION_COLUMN_NAME      = "_version_";
static const String TAG_COLUMN_NAME          = "_tag_"; // 0: upsert; 1: delete

static const ColId EXTRA_HANDLE_COLUMN_ID = -1;
static const ColId VERSION_COLUMN_ID      = -1024; // Prevent conflict with TiDB.
static const ColId TAG_COLUMN_ID          = -1025;

static DataTypePtr EXTRA_HANDLE_COLUMN_TYPE = DataTypeFactory::instance().get("Int64");
static DataTypePtr VERSION_COLUMN_TYPE      = DataTypeFactory::instance().get("UInt64");
static DataTypePtr TAG_COLUMN_TYPE          = DataTypeFactory::instance().get("UInt8");

static ColumnDefine VERSION_COLUMN_DEFINE{VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE};
static ColumnDefine TAG_COLUMN_DEFINE{TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE};

static constexpr Int64 MIN_INT64 = std::numeric_limits<Int64>::min();
static constexpr Int64 MAX_INT64 = std::numeric_limits<Int64>::max();

static constexpr Handle N_INF_HANDLE = MIN_INT64; // Use in range, indicating negative infinity.
static constexpr Handle P_INF_HANDLE = MAX_INT64; // Use in range, indicating positive infinity.

static_assert(static_cast<Int64>(static_cast<UInt64>(MIN_INT64)) == MIN_INT64, "Unsupported compiler!");
static_assert(static_cast<Int64>(static_cast<UInt64>(MAX_INT64)) == MAX_INT64, "Unsupported compiler!");

} // namespace DB
#pragma once

#include <Common/RWLock.h>

#include <vector>

namespace DB
{

using TableLockHolder = RWLock::LockHolder;
using TableLockHolders = std::vector<TableLockHolder>;

/// We use a "double lock" strategy, which stands for an "alter lock" plus a "drop lock" for the same table,
/// to achieve a fine-grained lock control for this given table.
/// This way, the lock dependencies among table operations like read/write, alter, and drop are minimized.
template <bool is_exclusive>
struct TableDoubleLockHolder
{
    // Release lock on `alter_lock` and return the ownership of `drop_lock`.
    // Once this function is invoked, should not access to this object again.
    [[nodiscard]] std::tuple<TableLockHolder, TableLockHolder> release() &&
    {
        return std::make_tuple(std::move(alter_lock), std::move(drop_lock));
    }

private:
    friend class IStorage;

    /// Order is important.
    TableLockHolder alter_lock;
    TableLockHolder drop_lock;
};

/// Table exclusive lock, holds write locks on both alter_lock and drop_lock of the table.
/// Useful for DROP-like queries that we want to ensure no more reading or writing or DDL
/// operations on that table.
using TableExclusiveLockHolder = TableDoubleLockHolder</*is_exclusive=*/true>;
/// Table structure lock, hold read locks on both alter_lock and drop_lock of the table.
/// Useful for decoding KV-pairs from Raft data that we want to ensure the structure
/// won't be changed. After decoding done, the caller can use `release` to release
/// the read lock on alter_lock but keep the drop_lock for writing blocks into
/// the table.
using TableStructureLockHolder = TableDoubleLockHolder</*is_exclusive=*/false>;

} // namespace DB

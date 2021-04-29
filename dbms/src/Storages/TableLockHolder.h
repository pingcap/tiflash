#pragma once

#include <Common/RWLock.h>

#include <vector>

namespace DB
{

using TableLockHolder = RWLock::LockHolder;
using TableLockHolders = std::vector<TableLockHolder>;

template <bool is_exclusive>
struct TableRWLocksHolder
{
    void release() { *this = TableRWLocksHolder<is_exclusive>(); }

    // Release lock on `drop_lock` and return the ownership of `drop_lock`.
    // Once this function is invoked, should not access to this object again.
    [[nodiscard]] TableLockHolder intoDropLock() &&
    {
        alter_lock.reset();
        return drop_lock;
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
using TableExclusiveLockHolder = TableRWLocksHolder</*is_exclusive=*/true>;
/// Table structure lock, hold read locks on both alter_lock and drop_lock of the table.
/// Useful for decoding KV-pairs from Raft data that we want to ensure the structure
/// won't be changed. After decoding done, the caller can use `releaseAlterLock` to
/// release the read lock on alter_lock but keep the drop_lock for writing blocks
/// into the table.
using TableStructureLockHolder = TableRWLocksHolder</*is_exclusive=*/false>;

} // namespace DB

#pragma once

#include <Common/RWLock.h>

#include <vector>

namespace DB
{

using TableLockHolder = RWLock::LockHolder;
using TableLockHolders = std::vector<TableLockHolder>;

/// Table exclusive lock, holds write locks on both alter_lock and drop_lock of the table.
/// Useful for DROP-like queries that we want to ensure no more reading or writing or DDL
/// operations on that table.
struct TableExclusiveLockHolder
{
    void release() { *this = TableExclusiveLockHolder(); }

private:
    friend class IStorage;

    /// Order is important.
    TableLockHolder alter_lock;
    TableLockHolder drop_lock;
};

/// Table structure lock, hold read locks on both alter_lock and drop_lock of the table.
/// Useful for decoding KV-pairs from Raft data that we want to ensure the structure
/// won't be changed. After decoding done, the caller can use `releaseAlterLock` to 
/// release the read lock on alter_lock but keep the drop_lock for writing blocks
/// into the table.
struct TableStructureLockHolder
{
    void releaseAlterLock() { alter_lock.reset(); }

    void release() { *this = TableStructureLockHolder(); }

private:
    friend class IStorage;

    /// Order is important.
    TableLockHolder alter_lock;
    TableLockHolder drop_lock;
};

} // namespace DB

#pragma once

#include <Common/RWLock.h>

#include <vector>

namespace DB
{

using TableLockHolder = RWLock::LockHolder;
using TableLockHolders = std::vector<TableLockHolder>;

/// Table exclusive lock, holds both alter and drop locks. Useful for DROP-like
/// queries.
struct TableExclusiveLockHolder
{
    void release() { *this = TableExclusiveLockHolder(); }

private:
    friend class IStorage;

    /// Order is important.
    TableLockHolder alter_lock;
    TableLockHolder drop_lock;
};

/// Table structure lock, hold both alter and drop read locks. Useful for decoding
/// KV-pairs from Raft data.
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

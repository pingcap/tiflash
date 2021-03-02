#pragma once

#include <Storages/Transaction/Types.h>

namespace DB
{

/// A quick-and-dirty copy of LockInfo structure in kvproto.
/// Used to transmit to client using non-ProtoBuf protocol.
struct LockInfo
{
    std::string primary_lock;
    UInt64 lock_version;
    std::string key;
    UInt64 lock_ttl;
};

using LockInfoPtr = std::unique_ptr<LockInfo>;

// To get lock info from region: read_tso is from cop request, any lock with ts in bypass_lock_ts should be filtered.
struct RegionLockReadQuery
{
    const UInt64 read_tso;
    const std::unordered_set<UInt64> * bypass_lock_ts;
};

} // namespace DB

#pragma once

#include <Storages/Transaction/Types.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/kvrpcpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
using LockInfoPtr = std::unique_ptr<kvrpcpb::LockInfo>;

// To get lock info from region: read_tso is from cop request, any lock with ts in bypass_lock_ts should be filtered.
struct RegionLockReadQuery
{
    const UInt64 read_tso;
    const std::unordered_set<UInt64> * bypass_lock_ts;
};

} // namespace DB

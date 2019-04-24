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
using LockInfos = std::vector<LockInfoPtr>;

} // namespace DB

#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>
#include <pingcap/kv/RegionCache.h>

namespace DB
{
using RegionVerID = pingcap::kv::RegionVerID;

class LockException : public Exception
{
public:
    explicit LockException(RegionID region_id_, LockInfoPtr lock_info) : region_id(region_id_), lock_info(std::move(lock_info)) {}

    RegionID region_id;
    LockInfoPtr lock_info;
};

} // namespace DB

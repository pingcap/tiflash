#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <pingcap/kv/RegionCache.h>
#pragma GCC diagnostic pop

namespace DB
{

class LockException : public Exception
{
public:
    explicit LockException(RegionID region_id_, LockInfoPtr lock_info) : region_id(region_id_), lock_info(std::move(lock_info)) {}

    RegionID region_id;
    LockInfoPtr lock_info;
};

} // namespace DB

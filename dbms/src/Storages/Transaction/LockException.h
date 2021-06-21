#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>

namespace DB
{

class LockException : public Exception
{
public:
    explicit LockException(const RegionVerID & region_ver_id_, LockInfoPtr lock_info)
        : region_ver_id(region_ver_id_), lock_info(std::move(lock_info))
    {}

    RegionVerID region_ver_id;
    LockInfoPtr lock_info;
};

} // namespace DB

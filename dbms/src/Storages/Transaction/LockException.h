#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>

namespace DB
{

class LockException : public Exception
{
public:
    explicit LockException(RegionID region_id_, LockInfos && lock_infos_) : region_id(region_id_), lock_infos(std::move(lock_infos_)) {}

    RegionID region_id;
    LockInfos lock_infos;
};

} // namespace DB

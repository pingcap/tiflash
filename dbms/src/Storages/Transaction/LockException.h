#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionLockInfo.h>

namespace DB
{

// TODO REVIEW: exception code?
class LockException : public Exception
{
public:
    explicit LockException(LockInfos && lock_infos_) : lock_infos(std::move(lock_infos_)) {}

    LockInfos lock_infos;
};

} // namespace DB

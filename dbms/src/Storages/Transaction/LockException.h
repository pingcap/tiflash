#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/Region.h>

namespace DB
{

// REVIEW: exception code? although nowhere should use the code
class LockException : public Exception
{
public:
    explicit LockException(Region::LockInfos && lock_infos_) : lock_infos(std::move(lock_infos_)) {}

    Region::LockInfos lock_infos;
};

} // namespace DB

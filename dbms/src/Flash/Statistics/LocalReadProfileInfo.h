#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>

namespace DB
{
struct LocalReadProfileInfo : public ConnectionProfileInfo
{
    LocalReadProfileInfo()
        : ConnectionProfileInfo("LocalRead")
    {}
};

using LocalReadProfileInfoPtr = std::shared_ptr<LocalReadProfileInfo>;
} // namespace DB
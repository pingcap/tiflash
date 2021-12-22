#pragma once

#include <common/types.h>

namespace DB
{
struct ConnectionProfileInfo
{
    Int64 packets = 0;
    Int64 bytes = 0;
};
} // namespace DB
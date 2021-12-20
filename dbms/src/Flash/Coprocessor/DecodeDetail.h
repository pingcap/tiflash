#pragma once

#include <common/types.h>

namespace DB
{
///
struct DecodeDetail
{
    Int64 rows = 0;
    UInt64 packet_bytes = 0;
};
} // namespace DB
#pragma once

#include <common/types.h>

namespace DB
{
struct DecodeChunksDetail
{
    UInt64 rows = 0;
    UInt64 blocks = 0;
    UInt64 bytes = 0;
};
} // namespace DB
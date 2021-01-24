#pragma once

#include <stdint.h>

extern "C" {

namespace DB
{
using ConstRawVoidPtr = const void *;
using RawVoidPtr = void *;
using RegionId = uint64_t;
using RaftStoreProxyPtr = ConstRawVoidPtr;
} // namespace DB
}

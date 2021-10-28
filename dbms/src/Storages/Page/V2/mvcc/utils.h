#pragma once

#include <Core/Types.h>

#include <random>

namespace DB::PS::V2::MVCC::utils
{
UInt32 randInt(const UInt32 min, const UInt32 max);

} // namespace DB::PS::V2::MVCC::utils

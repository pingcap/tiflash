#pragma once

#include <Core/Types.h>

#include <random>

namespace DB
{
namespace MVCC
{
namespace utils
{

UInt32 randInt(const UInt32 min, const UInt32 max);

}
} // namespace MVCC
} // namespace DB

#pragma once

#include <Columns/ColumnString.h>

namespace DB
{
namespace StringUtil
{
/// Same as ColumnString's private offsetAt and sizeAt.
static size_t ALWAYS_INLINE offsetAt(const ColumnString::Offsets & offsets, size_t i)
{
    return i == 0 ? 0 : offsets[i - 1];
}

static size_t ALWAYS_INLINE sizeAt(const ColumnString::Offsets & offsets, size_t i)
{
    return i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1]);
}
} // namespace StringUtil
} // namespace DB

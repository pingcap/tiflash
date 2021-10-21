#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <iterator>

namespace DB
{
template <typename... Args>
inline void fmtAppend(fmt::memory_buffer & buffer, Args... args)
{
    fmt::format_to(std::back_inserter(buffer), args...);
}
} // namespace DB

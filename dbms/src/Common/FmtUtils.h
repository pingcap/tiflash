#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <iterator>
#include <string>

namespace DB
{
class FmtBuffer
{
public:
    FmtBuffer() = default;

    template <typename... Args>
    FmtBuffer & append(Args... args)
    {
        fmt::format_to(std::back_inserter(buffer), std::forward<Args>(args)...);
        return *this;
    }

    std::string toString() const
    {
        return fmt::to_string(buffer);
    }

    void resize(size_t count) { buffer.resize(count); }
    void reserve(size_t capacity) { buffer.reserve(capacity); }
    void clear() { buffer.clear(); }

private:
    fmt::memory_buffer buffer;
};
} // namespace DB

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
    FmtBuffer & fmtAppend(Args... args)
    {
        fmt::format_to(std::back_inserter(buffer), std::forward<Args>(args)...);
        return *this;
    }

    FmtBuffer & append(const std::string & s)
    {
        size_t old_size = buffer.size();
        size_t size_to_append = s.size();
        resize(old_size + size_to_append);
        char * ptr = buffer.data();
        memcpy(ptr + old_size, s.c_str(), size_to_append);
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

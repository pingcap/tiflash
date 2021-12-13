#pragma once

#include <common/StringRef.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <cstddef>
#include <iterator>
#include <string>
#include <type_traits>

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

    FmtBuffer & append(StringRef s)
    {
        buffer.append(s.data, s.data + s.size);
        return *this;
    }

    std::string toString() const
    {
        return fmt::to_string(buffer);
    }

    template <typename Iter>
    FmtBuffer & joinStr(
        Iter first,
        Iter end,
        StringRef delimiter = ", ")
    {
        if (first == end)
            return *this;
        append(*first);
        ++first;
        for (; first != end; ++first)
        {
            append(delimiter);
            append(*first);
        }
        return *this;
    }

    template <typename Iter, typename FF>
    FmtBuffer & joinStr(
        Iter first,
        Iter end,
        StringRef delimiter = ", ",
        FF && toStringFunc = [](const auto & s, FmtBuffer & fb) { fb.append(s); })
    {
        if (first == end)
            return *this;
        toStringFunc(*first, *this);
        ++first;
        for (; first != end; ++first)
        {
            append(delimiter);
            toStringFunc(*first, *this);
        }
        return *this;
    }


    void resize(size_t count) { buffer.resize(count); }
    void reserve(size_t capacity) { buffer.reserve(capacity); }
    void clear() { buffer.clear(); }

private:
    fmt::memory_buffer buffer;
};
} // namespace DB

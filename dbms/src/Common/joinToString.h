#pragma once

#include <IO/Operators.h>
#include <IO/WriteBuffer.h>

#include <functional>

namespace DB
{
template <typename Iter, typename FF>
inline void joinIter(
    Iter iter,
    Iter end,
    WriteBuffer & buf [[maybe_unused]],
    FF toStringFunc [[maybe_unused]],
    const std::string & delimiter [[maybe_unused]] = ", ")
{
    if (iter == end)
    {
        return;
    }

    toStringFunc(*iter++, buf);
    for (; iter != end; ++iter)
    {
        buf << delimiter;
        toStringFunc(*iter, buf);
    }
}
} // namespace DB
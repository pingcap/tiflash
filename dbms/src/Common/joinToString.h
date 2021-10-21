#pragma once

#include <Common/FmtUtils.h>

#include <functional>
#include <iterator>
#include <string>

namespace DB
{
template <typename Iter, typename FF>
inline void joinIter(
    Iter iter,
    Iter end,
    FmtBuffer & buf [[maybe_unused]],
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
        buf.append(delimiter);
        toStringFunc(*iter, buf);
    }
}
} // namespace DB
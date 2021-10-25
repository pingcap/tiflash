#pragma once

#include <Common/FmtUtils.h>
#include <common/StringRef.h>

#include <functional>
#include <iterator>
#include <string>

namespace DB
{
template <typename Iter, typename FF>
inline void joinIter(
    Iter first,
    Iter end,
    FmtBuffer & buf,
    FF && toStringFunc,
    StringRef delimiter = ", ")
{
    if (first == end)
    {
        return;
    }

    toStringFunc(*first, buf);
    ++first;
    for (; first != end; ++first)
    {
        buf.append(delimiter);
        toStringFunc(*first, buf);
    }
}
} // namespace DB
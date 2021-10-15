#pragma once

#include <functional>

namespace DB
{
template <typename Iter, typename FF>
void dumpIter(
    Iter iter,
    Iter end,
    std::ostream & ostr [[maybe_unused]],
    FF f [[maybe_unused]],
    const std::string & delimiter [[maybe_unused]] = ", ")
{
    if (iter == end)
    {
        return;
    }

    f(*iter++, ostr);
    for (; iter != end; ++iter)
    {
        ostr << delimiter;
        f(*iter, ostr);
    }
}
} // namespace DB
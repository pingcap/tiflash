#pragma once

#include <Core/Types.h>

#include <Storages/Transaction/TiKVHandle.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace CHTableHandle
{
inline void merge_ranges(std::vector<DB::HandleRange> & ranges)
{
    if (ranges.empty())
        return;

    // ranged may overlap, should merge them
    std::sort(ranges.begin(), ranges.end());
    size_t size = 0;
    for (size_t i = 1; i < ranges.size(); ++i)
    {
        if (ranges[i].first <= ranges[size].second)
            ranges[size].second = std::max(ranges[i].second, ranges[size].second);
        else
            ranges[++size] = ranges[i];
    }
    size = size + 1;
    ranges.resize(size);
}
} // namespace CHTableHandle

} // namespace DB

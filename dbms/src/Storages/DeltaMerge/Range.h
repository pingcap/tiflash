#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
template <typename T, bool right_open>
struct Range
{
    static constexpr T MIN = std::numeric_limits<T>::min();
    static constexpr T MAX = std::numeric_limits<T>::max();

    T start;
    T end;

    void swap(Range & other)
    {
        std::swap(start, other.start);
        std::swap(end, other.end);
    }

    static Range newAll() { return {MIN, MAX}; }
    static Range newNone()
    {
        if constexpr (right_open)
            return {0, 0};
        else
            throw Exception("Logical error"); // We should not have none range for version range.
    }

    static Range startFrom(T start_) { return {start_, MAX}; }
    static Range endWith(T end_) { return {MIN, end_}; }

    inline bool all() const { return start == MIN && end == MAX; }
    inline bool none() const
    {
        if constexpr (right_open)
            return start >= end;
        else
            return start > end;
    }

    inline bool check(T value) const
    {
        if constexpr (right_open)
            return (start == MIN || start <= value) && (end == MAX || value < end);
        else
            return (start == MIN || start <= value) && (end == MAX || value <= end);
    }
};

using HandleRange  = Range<Handle, true>;
static_assert(std::is_pod_v<HandleRange>, "HandleRange should be a pod");
} // namespace DB
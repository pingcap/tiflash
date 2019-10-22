#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{

template <typename T, bool right_open>
struct Range
{
    static constexpr T MIN = std::numeric_limits<T>::min();
    static constexpr T MAX = std::numeric_limits<T>::max();

    T start;
    T end;

    Range(T start_, T end_) : start(start_), end(end_) {}
    Range() : start(0), end(0) {}

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

using HandleRange = Range<Handle, true>;

} // namespace DM
} // namespace DB
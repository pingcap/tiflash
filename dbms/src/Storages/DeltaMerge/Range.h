#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{
namespace DM
{

template <typename T>
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
    static Range newNone() { return {0, 0}; }

    static Range startFrom(T start_) { return {start_, MAX}; }
    static Range endWith(T end_) { return {MIN, end_}; }

    inline bool all() const { return start == MIN && end == MAX; }
    inline bool none() const { return start >= end; }

    inline Range shrink(const Range<T> & other) const { return Range(std::max(start, other.start), std::min(end, other.end)); }

    inline bool intersect(const Range<T> & other) const { return std::max(other.start, start) < std::min(other.end, end); }

    // [first, last_include]
    inline bool intersect(T first, T last_include) const
    {
        T max_start = std::max(first, start);
        return (last_include >= end && checkEnd(max_start)) || (last_include < end && max_start <= last_include);
    }

    // [first, last_include]
    inline bool include(T first, T last_include) const { return check(first) && check(last_include); }

    inline bool checkStart(T value) const { return start == MIN || start <= value; }

    inline bool checkEnd(T value) const { return end == MAX || value < end; }

    inline bool check(T value) const { return checkStart(value) && checkEnd(value); }

    inline String toString() const { return rangeToString(*this); }
};

using HandleRange  = Range<Handle>;
using HandleRanges = std::vector<HandleRange>;

} // namespace DM
} // namespace DB
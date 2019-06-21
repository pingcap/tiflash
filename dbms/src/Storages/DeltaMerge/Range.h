#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

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
            throw Exception("Logical error");
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

    inline Range shrink(const Range<T, right_open> & other) const { return Range(std::max(start, other.start), std::min(end, other.end)); }

    // [first, last]
    inline bool intersect(T first, T last) const
    {
        if (last >= end)
            return !Range<T, true>(std::max(first, start), std::min(last, end)).none();
        else
            return !Range<T, right_open>(std::max(first, start), std::min(last, end)).none();
    }

    // [first, last]
    inline bool include(T first, T last) const
    {
        bool ok = std::max(first, start) == first && std::min(last, end) == last;
        if constexpr (right_open)
            ok = ok && last != end;
        return ok;
    }

    inline bool checkStart(T value) const { return start == MIN || start <= value; }

    inline bool checkEnd(T value) const
    {
        if constexpr (right_open)
            return end == MAX || value < end;
        else
            return end == MAX || value <= end;
    }

    inline bool check(T value) const { return checkStart(value) && checkEnd(value); }

    inline String toString() const { return rangeToString(*this); }
};

using HandleRange  = Range<Handle, true>;
using HandleRanges = std::vector<HandleRange>;

} // namespace DM
} // namespace DB
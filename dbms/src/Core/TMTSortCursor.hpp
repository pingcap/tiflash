#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/SortCursor.h>

namespace DB
{

union TMTCmpOptimizedRes
{
    Int32 all;
    std::array<Int8, 4> diffs;
};

/// optimize SortCursor for TMT engine which must have 3 column: PK, VERSION, DELMARK.
struct TMTSortCursor
{
    SortCursorImpl * impl = nullptr;

    TMTSortCursor() {}
    TMTSortCursor(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator->() { return impl; }
    const SortCursorImpl * operator->() const { return impl; }

    bool none() { return !impl; }
    bool operator==(const SortCursor & other) const { return impl == other.impl; }
    bool operator!=(const SortCursor & other) const { return impl != other.impl; }

    static inline TMTCmpOptimizedRes cmp(
        const ColumnRawPtrs & lsort_columns, const size_t lhs_pos, const ColumnRawPtrs & rsort_columns, const size_t rhs_pos)
    {
        TMTCmpOptimizedRes res{.all = 0};
        {
            res.diffs[0] = lsort_columns[0]->compareAt(lhs_pos, rhs_pos, *(rsort_columns[0]), 0);
        }
        {
            UInt64 t1 = static_cast<const ColumnUInt64 *>(lsort_columns[1])->getElement(lhs_pos);
            UInt64 t2 = static_cast<const ColumnUInt64 *>(rsort_columns[1])->getElement(rhs_pos);
            res.diffs[1] = t1 == t2 ? 0 : (t1 > t2 ? 1 : -1);
        }
        {
            UInt8 d1 = static_cast<const ColumnUInt8 *>(lsort_columns[2])->getElement(lhs_pos);
            UInt8 d2 = static_cast<const ColumnUInt8 *>(rsort_columns[2])->getElement(rhs_pos);
            res.diffs[2] = d1 == d2 ? 0 : (d1 > d2 ? 1 : -1);
        }

        return res;
    }

    TMTCmpOptimizedRes cmp(const TMTSortCursor & rhs, const size_t lhs_pos, const size_t rhs_pos) const
    {
        return cmp(impl->sort_columns, lhs_pos, rhs.impl->sort_columns, rhs_pos);
    }

    bool greaterAt(const TMTSortCursor & rhs, const size_t lhs_pos, const size_t rhs_pos) const
    {
        auto res = cmp(rhs, lhs_pos, rhs_pos);
        return greaterAt(res);
    }

    static inline bool greaterAt(const TMTCmpOptimizedRes res)
    {
        return res.diffs[0] > 0
            ? true
            : (res.diffs[0] < 0 ? false : (res.diffs[1] > 0 ? true : (res.diffs[1] < 0 ? false : (res.diffs[2] > 0 ? true : false))));
    }

    bool totallyLessOrEquals(const TMTSortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const TMTSortCursor & rhs) const { return greaterAt(rhs, impl->pos, rhs.impl->pos); }

    bool operator<(const TMTSortCursor & rhs) const { return greater(rhs); }
};

} // namespace DB

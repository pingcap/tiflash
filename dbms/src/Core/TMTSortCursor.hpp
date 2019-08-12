#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/SortCursor.h>
#include <Core/TMTPKType.h>

namespace DB
{

union TMTCmpOptimizedRes
{
    Int32 all;
    std::array<Int8, 4> diffs;
};

static_assert(sizeof(TMTCmpOptimizedRes) == 4);

/// type of pk column will be int64, uint64 and others(int32, int8, uint32 ...).
/// type of version column is uint64.
/// type of delmark column is uint8.
/// order of sorting will always be pk -> version -> delmark
template <bool just_diff, bool only_pk, TMTPKType pk_type>
inline TMTCmpOptimizedRes cmpTMTCursor(
    const ColumnRawPtrs & lsort_columns, const size_t lhs_pos, const ColumnRawPtrs & rsort_columns, const size_t rhs_pos)
{
    TMTCmpOptimizedRes res{.all = 0};

    // PK
    if constexpr (pk_type == TMTPKType::INT64)
    {
        auto h1 = static_cast<const ColumnInt64 *>(lsort_columns[0])->getElement(lhs_pos);
        auto h2 = static_cast<const ColumnInt64 *>(rsort_columns[0])->getElement(rhs_pos);

        if constexpr (just_diff)
        {
            res.diffs[0] = h1 != h2;
        }
        else
        {
            res.diffs[0] = h1 == h2 ? 0 : (h1 > h2 ? 1 : -1);
        }
    }
    else if constexpr (pk_type == TMTPKType::UINT64)
    {
        auto h1 = static_cast<const ColumnUInt64 *>(lsort_columns[0])->getElement(lhs_pos);
        auto h2 = static_cast<const ColumnUInt64 *>(rsort_columns[0])->getElement(rhs_pos);

        if constexpr (just_diff)
        {
            res.diffs[0] = h1 != h2;
        }
        else
        {
            res.diffs[0] = h1 == h2 ? 0 : (h1 > h2 ? 1 : -1);
        }
    }
    else
    {
        res.diffs[0] = lsort_columns[0]->compareAt(lhs_pos, rhs_pos, *(rsort_columns[0]), 0);
    }

    if constexpr (only_pk)
    {
        return res;
    }

    // VERSION
    {
        auto t1 = static_cast<const ColumnUInt64 *>(lsort_columns[1])->getElement(lhs_pos);
        auto t2 = static_cast<const ColumnUInt64 *>(rsort_columns[1])->getElement(rhs_pos);

        if constexpr (just_diff)
        {
            res.diffs[1] = t1 != t2;
        }
        else
        {
            res.diffs[1] = t1 == t2 ? 0 : (t1 > t2 ? 1 : -1);
        }
    }

    // DELMARK
    {
        auto d1 = static_cast<const ColumnUInt8 *>(lsort_columns[2])->getElement(lhs_pos);
        auto d2 = static_cast<const ColumnUInt8 *>(rsort_columns[2])->getElement(rhs_pos);

        if constexpr (just_diff)
        {
            res.diffs[2] = d1 != d2;
        }
        else
        {
            res.diffs[2] = d1 == d2 ? 0 : (d1 > d2 ? 1 : -1);
        }
    }

    return res;
}


/// optimize SortCursor for TMT engine which must have 3 column: PK, VERSION, DELMARK.
template <bool only_pk = false, TMTPKType pk_type = TMTPKType::UNSPECIFIED>
struct TMTSortCursor
{
    SortCursorImpl * impl = nullptr;

    TMTSortCursor() {}
    TMTSortCursor(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator->() { return impl; }
    const SortCursorImpl * operator->() const { return impl; }

    bool none() { return !impl; }

    bool isSame(const TMTSortCursor & other) const { return impl == other.impl; }
    bool notSame(const TMTSortCursor & other) const { return impl != other.impl; }

    TMTCmpOptimizedRes cmpIgnOrder(const TMTSortCursor & rhs, const size_t lhs_pos, const size_t rhs_pos) const
    {
        return cmpTMTCursor<false, only_pk, pk_type>(impl->sort_columns, lhs_pos, rhs.impl->sort_columns, rhs_pos);
    }

    bool greaterAt(const TMTSortCursor & rhs, const size_t lhs_pos, const size_t rhs_pos) const
    {
        auto res = cmpIgnOrder(rhs, lhs_pos, rhs_pos);

        if constexpr (only_pk)
        {
            return res.diffs[0] > 0 ? true : (res.diffs[0] < 0 ? false : (impl->order > rhs.impl->order));
        }

        return greaterAt(res, impl->order, rhs.impl->order);
    }

    bool lessAtIgnOrder(const TMTSortCursor & rhs, const size_t lhs_pos, const size_t rhs_pos) const
    {
        auto res = cmpIgnOrder(rhs, lhs_pos, rhs_pos);

        if constexpr (only_pk)
            return res.diffs[0] < 0;

        return lessAtIgnOrder(res);
    }

    static inline bool greaterAt(const TMTCmpOptimizedRes res, const size_t lorder, const size_t rorder)
    {
        return res.diffs[0] > 0
            ? true
            : (res.diffs[0] < 0
                      ? false
                      : (res.diffs[1] > 0
                                ? true
                                : (res.diffs[1] < 0 ? false : (res.diffs[2] > 0 ? true : (res.diffs[2] < 0 ? false : (lorder > rorder))))));
    }

    static inline bool lessAtIgnOrder(const TMTCmpOptimizedRes res)
    {
        return res.diffs[0] < 0
            ? true
            : (res.diffs[0] > 0 ? false : (res.diffs[1] < 0 ? true : (res.diffs[1] > 0 ? false : (res.diffs[2] < 0 ? true : false))));
    }

    bool totallyLessOrEquals(const TMTSortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const TMTSortCursor & rhs) const { return greaterAt(rhs, impl->pos, rhs.impl->pos); }

    bool totallyLessIgnOrder(const TMTSortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;
        return lessAtIgnOrder(rhs, impl->rows - 1, 0);
    }

    bool operator<(const TMTSortCursor & rhs) const { return greater(rhs); }
};

using TMTSortCursorInt64PK = TMTSortCursor<true, TMTPKType::INT64>;
using TMTSortCursorUInt64PK = TMTSortCursor<true, TMTPKType::UINT64>;
using TMTSortCursorUnspecifiedPK = TMTSortCursor<true, TMTPKType::UNSPECIFIED>;

using TMTSortCursorInt64 = TMTSortCursor<false, TMTPKType::INT64>;
using TMTSortCursorUInt64 = TMTSortCursor<false, TMTPKType::UINT64>;
using TMTSortCursorUnspecified = TMTSortCursor<false, TMTPKType::UNSPECIFIED>;

} // namespace DB

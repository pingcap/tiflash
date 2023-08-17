// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>


namespace DB
{
/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct SortCursorImpl
{
    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    size_t pos = 0;
    size_t rows = 0;

    /** Determines order if comparing columns are equal.
      * Order is determined by number of cursor.
      *
      * Cursor number (always?) equals to number of merging part.
      * Therefore this field can be used to determine part number of current row (see ColumnGathererStream).
      */
    size_t order;

    using NeedCollationFlags = std::vector<UInt8>;

    /** Should we use Collator to sort a column? */
    NeedCollationFlags need_collation;

    /** Is there at least one column with Collator. */
    bool has_collation = false;

    SortCursorImpl()
        : order(0)
    {}

    SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0)
        : desc(desc_)
        , sort_columns_size(desc.size())
        , order(order_)
        , need_collation(desc.size())
    {
        reset(block);
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block)
    {
        all_columns.clear();
        sort_columns.clear();

        size_t num_columns = block.columns();

        for (size_t j = 0; j < num_columns; ++j)
            all_columns.push_back(block.safeGetByPosition(j).column.get());

        for (size_t j = 0, size = desc.size(); j < size; ++j)
        {
            size_t column_number
                = !desc[j].column_name.empty() ? block.getPositionByName(desc[j].column_name) : desc[j].column_number;

            sort_columns.push_back(block.safeGetByPosition(column_number).column.get());

            need_collation[j] = desc[j].collator != nullptr
                && typeid_cast<const ColumnString *>(std::get<0>(removeNullable(sort_columns.back())));
            has_collation |= need_collation[j];
        }

        pos = 0;
        rows = all_columns[0]->size();
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    void next() { ++pos; }
};


/// For easy copying.
struct SortCursor
{
    SortCursorImpl * impl = nullptr;

    SortCursor() = default;
    explicit SortCursor(SortCursorImpl * impl_)
        : impl(impl_)
    {}
    SortCursorImpl * operator->() { return impl; } // NOLINT(readability-make-member-function-const)
    const SortCursorImpl * operator->() const { return impl; }

    bool none() const { return !impl; }
    bool operator==(const SortCursor & other) const { return impl == other.impl; }
    bool operator!=(const SortCursor & other) const { return impl != other.impl; }

    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction
                * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        return impl->order > rhs.impl->order;
    }

    bool lessAtIgnOrder(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction
                * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
            if (res < 0)
                return true;
            if (res > 0)
                return false;
        }
        return false;
    }

    bool equalAtIgnOrder(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction
                * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
            if (res != 0)
                return false;
        }
        return true;
    }

    bool totallyLessIgnOrder(const SortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;
        return lessAtIgnOrder(rhs, impl->rows - 1, 0);
    }

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool totallyLessOrEquals(const SortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const SortCursor & rhs) const { return greaterAt(rhs, impl->pos, rhs.impl->pos); }

    bool equalIgnOrder(const SortCursor & rhs) const { return equalAtIgnOrder(rhs, impl->pos, rhs.impl->pos); }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator<(const SortCursor & rhs) const { return greater(rhs); }
};


/// Separate comparator for locale-sensitive string comparisons
struct SortCursorWithCollation
{
    SortCursorImpl * impl = nullptr;

    SortCursorWithCollation() = default;
    explicit SortCursorWithCollation(SortCursorImpl * impl_)
        : impl(impl_)
    {}
    SortCursorImpl * operator->() { return impl; } // NOLINT(readability-make-member-function-const)
    const SortCursorImpl * operator->() const { return impl; }

    bool none() const { return !impl; }
    bool operator==(const SortCursor & other) const { return impl == other.impl; }
    bool operator!=(const SortCursor & other) const { return impl != other.impl; }

    bool greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res;
            if (impl->need_collation[i])
                res = impl->sort_columns[i]->compareAt(
                    lhs_pos,
                    rhs_pos,
                    *(rhs.impl->sort_columns[i]),
                    nulls_direction,
                    *impl->desc[i].collator);
            else
                res = impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);

            res *= direction;
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        return impl->order > rhs.impl->order;
    }

    bool equalAtIgnOrder(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res;
            if (impl->need_collation[i])
            {
                res = impl->sort_columns[i]->compareAt(
                    lhs_pos,
                    rhs_pos,
                    *(rhs.impl->sort_columns[i]),
                    nulls_direction,
                    *impl->desc[i].collator);
            }
            else
                res = impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);

            res *= direction;
            if (res != 0)
                return false;
        }
        return true;
    }

    bool totallyLessOrEquals(const SortCursorWithCollation & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const SortCursorWithCollation & rhs) const { return greaterAt(rhs, impl->pos, rhs.impl->pos); }

    bool equalIgnOrder(const SortCursorWithCollation & rhs) const
    {
        return equalAtIgnOrder(rhs, impl->pos, rhs.impl->pos);
    }

    bool operator<(const SortCursorWithCollation & rhs) const { return greater(rhs); }
};

} // namespace DB

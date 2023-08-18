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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


FilterBlockInputStream::FilterBlockInputStream(
    const BlockInputStreamPtr & input,
    const ExpressionActionsPtr & expression_,
    const String & filter_column_name,
    const String & req_id)
    : expression(expression_)
    , log(Logger::get(NAME, req_id))
{
    children.push_back(input);

    /// Determine position of filter column.
    header = input->getHeader();
    expression->execute(header);

    filter_column = header.getPositionByName(filter_column_name);
    auto & column_elem = header.safeGetByPosition(filter_column);

    /// Isn't the filter already constant?
    if (column_elem.column)
        constant_filter_description = ConstantFilterDescription(*column_elem.column);

    if (!constant_filter_description.always_false && !constant_filter_description.always_true)
    {
        /// Replace the filter column to a constant with value 1.
        FilterDescription filter_description_check(*column_elem.column);
        column_elem.column = column_elem.type->createColumnConst(header.rows(), UInt64(1));
    }
}

Block FilterBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
}


Block FilterBlockInputStream::getHeader() const
{
    return header;
}


Block FilterBlockInputStream::readImpl()
{
    Block res;

    if (constant_filter_description.always_false)
        return res;

    /// Until non-empty block after filtering or end of stream.
    while (true)
    {
        IColumn::Filter * child_filter = nullptr;

        res = children.back()->read(child_filter, true);

        if (!res)
            return res;

        expression->execute(res);

        if (constant_filter_description.always_true && !child_filter)
            return res;

        size_t columns = res.columns();
        size_t rows = res.rows();
        ColumnPtr column_of_filter = res.safeGetByPosition(filter_column).column;

        if (unlikely(child_filter && child_filter->size() != rows))
            throw Exception("Unexpected child filter size", ErrorCodes::LOGICAL_ERROR);

        /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
            *  and now - are calculated. That is, not all cases are covered by the code above.
            * This happens if the function returns a constant for a non-constant argument.
            * For example, `ignore` function.
            */
        constant_filter_description = ConstantFilterDescription(*column_of_filter);

        if (constant_filter_description.always_false)
        {
            res.clear();
            return res;
        }

        IColumn::Filter * filter;
        ColumnPtr filter_holder;

        if (constant_filter_description.always_true)
        {
            if (child_filter)
                filter = child_filter;
            else
                return res;
        }
        else
        {
            FilterDescription filter_and_holder(*column_of_filter);
            filter = const_cast<IColumn::Filter *>(filter_and_holder.data);
            filter_holder = filter_and_holder.data_holder;

            if (child_filter)
            {
                /// Merge child_filter
                UInt8 * a = filter->data();
                UInt8 * b = child_filter->data();
                for (size_t i = 0; i < rows; ++i)
                {
                    *a = *a > 0 && *b != 0;
                    ++a;
                    ++b;
                }
            }
        }

        /** Let's find out how many rows will be in result.
          * To do this, we filter out the first non-constant column
          *  or calculate number of set bytes in the filter.
          */
        size_t first_non_constant_column = 0;
        for (size_t i = 0; i < columns; ++i)
        {
            if (!res.safeGetByPosition(i).column->isColumnConst())
            {
                first_non_constant_column = i;

                if (first_non_constant_column != static_cast<size_t>(filter_column))
                    break;
            }
        }

        size_t filtered_rows = 0;
        if (first_non_constant_column != static_cast<size_t>(filter_column))
        {
            ColumnWithTypeAndName & current_column = res.safeGetByPosition(first_non_constant_column);
            current_column.column = current_column.column->filter(*filter, -1);
            filtered_rows = current_column.column->size();
        }
        else
        {
            filtered_rows = countBytesInFilter(*filter);
        }

        /// If the current block is completely filtered out, let's move on to the next one.
        if (filtered_rows == 0)
            continue;

        /// If all the rows pass through the filter.
        if (filtered_rows == rows)
        {
            /// Replace the column with the filter by a constant.
            res.safeGetByPosition(filter_column).column
                = res.safeGetByPosition(filter_column).type->createColumnConst(filtered_rows, UInt64(1));
            /// No need to touch the rest of the columns.
            return res;
        }

        /// Filter the rest of the columns.
        for (size_t i = 0; i < columns; ++i)
        {
            ColumnWithTypeAndName & current_column = res.safeGetByPosition(i);

            if (i == static_cast<size_t>(filter_column))
            {
                /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
                /// NOTE User could pass column with something different than 0 and 1 for filter.
                /// Example:
                ///  SELECT materialize(100) AS x WHERE x
                /// will work incorrectly.
                current_column.column = current_column.type->createColumnConst(filtered_rows, UInt64(1));
                continue;
            }

            if (i == first_non_constant_column)
                continue;

            if (current_column.column->isColumnConst())
                current_column.column = current_column.column->cut(0, filtered_rows);
            else
                current_column.column = current_column.column->filter(*filter, filtered_rows);
        }

        return res;
    }
}


} // namespace DB

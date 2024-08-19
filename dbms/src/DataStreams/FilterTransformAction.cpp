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
#include <DataStreams/FilterTransformAction.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


FilterTransformAction::FilterTransformAction(
    const Block & header_,
    const ExpressionActionsPtr & expression_,
    const String & filter_column_name)
    : header(header_)
    , expression(expression_)
{
    /// Determine position of filter column.
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
        column_elem.column = column_elem.type->createColumnConst(header.rows(), static_cast<UInt64>(1));
    }
}

bool FilterTransformAction::alwaysFalse() const
{
    return constant_filter_description.always_false;
}

Block FilterTransformAction::getHeader() const
{
    return header;
}

ExpressionActionsPtr FilterTransformAction::getExperssion() const
{
    return expression;
}

bool FilterTransformAction::transform(Block & block, FilterPtr & res_filter, bool return_filter)
{
    if (unlikely(!block))
        return true;

    if (block.getRSResult().allMatch())
    {
        block.insert(header.getPositionByName(filter_column), header.safeGetByPosition(filter_column)); // Make some checks on block structure happy.
        if (return_filter)
            res_filter = nullptr;
        return true;
    }

    expression->execute(block);

    if (constant_filter_description.always_true)
    {
        if (return_filter)
            res_filter = nullptr;
        return true;
    }

    size_t columns = block.columns();
    size_t rows = block.rows();
    ColumnPtr column_of_filter = block.safeGetByPosition(filter_column).column;

    /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
        *  and now - are calculated. That is, not all cases are covered by the code above.
        * This happens if the function returns a constant for a non-constant argument.
        * For example, `ignore` function.
        */
    constant_filter_description = ConstantFilterDescription(*column_of_filter);

    if (constant_filter_description.always_false)
    {
        block.clear();
        return true;
    }

    if (constant_filter_description.always_true)
    {
        if (return_filter)
            res_filter = nullptr;
        return true;
    }
    else
    {
        FilterDescription filter_and_holder(*column_of_filter);
        filter = const_cast<IColumn::Filter *>(filter_and_holder.data);
        filter_holder = filter_and_holder.data_holder;
    }

    if (return_filter)
    {
        res_filter = filter;
        return true;
    }

    size_t filtered_rows = countBytesInFilter(*filter);

    /// If the current block is completely filtered out, let's move on to the next one.
    if (filtered_rows == 0)
        return false;

    /// If all the rows pass through the filter.
    if (filtered_rows == rows)
    {
        /// Replace the column with the filter by a constant.
        block.safeGetByPosition(filter_column).column
            = block.safeGetByPosition(filter_column).type->createColumnConst(filtered_rows, static_cast<UInt64>(1));
        /// No need to touch the rest of the columns.
        return true;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName & current_column = block.safeGetByPosition(i);

        if (i == filter_column)
        {
            /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
            /// NOTE User could pass column with something different than 0 and 1 for filter.
            /// Example:
            ///  SELECT materialize(100) AS x WHERE x
            /// will work incorrectly.
            current_column.column = current_column.type->createColumnConst(filtered_rows, static_cast<UInt64>(1));
            continue;
        }

        if (current_column.column->isColumnConst())
            current_column.column = current_column.column->cut(0, filtered_rows);
        else
            current_column.column = current_column.column->filter(*filter, filtered_rows);
    }

    return true;
}
} // namespace DB

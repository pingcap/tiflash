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

#include <Columns/ColumnUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Expand.h>
#include <tipb/executor.pb.h>

namespace DB
{

Expand::Expand(const DB::GroupingSets & gss)
    : group_sets_names(gss)
{
    collectNameSet();
}

String Expand::getGroupingSetsDes() const
{
    FmtBuffer buffer;
    buffer.append("[");
    for (const auto & grouping_set : group_sets_names)
    {
        buffer.append("{");
        for (const auto & grouping_exprs : grouping_set)
        {
            buffer.append("<");
            buffer.joinStr(grouping_exprs.begin(), grouping_exprs.end());
            buffer.append(">");
        }
        buffer.append("}");
    }
    buffer.append("]");
    return buffer.toString();
}

/// for cases like: select count(distinct a), count(distinct b) from t;
/// it will generate 2 group set with <a> and <b>, over which we should
/// expand one more replica of the source rows from the input block and
/// identify it with the grouping id in the appended new column.
///
/// eg: source block         ==>        replicated block
///      <a, b>              ==>         <a, b, groupingID>  a new column is appended
///      1  1       target a -+----->     1  null  groupingID for a =1
///      2  2                 +----->     2  null  groupingID for b =2
///                 target b -+----->     null  1  groupingID for a =1
///                           +----->     null  2  groupingID for b =2
///
/// when target a specified group set, other group set columns should be filled
/// with null value to make group by(a,b) operator to meet the equivalence effect
/// of group by(a) and group by(b) since the other group set columns has been filled
/// with null value.
///
/// \param input the source block
/// \return

void Expand::replicateAndFillNull(Block & block) const
{
    size_t origin_rows = block.rows();
    // make a replicate slice, using it to replicate origin rows.
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    offsets_to_replicate = std::make_unique<IColumn::Offsets>(origin_rows);

    // get the replicate offset fixed as group set num.
    IColumn::Offset current_offset = 0;
    const IColumn::Offset replicate_times_for_one_row = getGroupSetNum();

    // create a column for grouping id.
    auto grouping_id_column = ColumnUInt64::create();
    auto & grouping_id_column_data = grouping_id_column->getData();
    // reserve N times of current block rows size.
    grouping_id_column_data.resize(origin_rows * replicate_times_for_one_row);

    size_t grouping_id_column_index = 0;
    for (size_t i = 0; i < origin_rows; ++i)
    {
        current_offset += replicate_times_for_one_row;
        (*offsets_to_replicate)[i] = current_offset;

        // in the same loop, to fill the grouping id.
        for (UInt64 j = 0; j < replicate_times_for_one_row; ++j)
        {
            // start from 1.
            grouping_id_column_data[grouping_id_column_index++] = j + 1;
        }
    }

    // replicate the original block rows.
    size_t existing_columns = block.columns();

    if (offsets_to_replicate)
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            // expand the origin const column, since it may be filled with null value when expanding.
            if (block.safeGetByPosition(i).column->isColumnConst())
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->convertToFullColumnIfConst();

            // for every existing column, if the column is a grouping set column, make it nullable.
            auto & column = block.safeGetByPosition(i);
            if (isInGroupSetColumn(column.name) && !column.column->isColumnNullable())
                convertColumnToNullable(block.getByPosition(i));

            if (!offsets_to_replicate->empty())
                column.column = column.column->replicate(*offsets_to_replicate);
        }
    }


    // after replication, it just copied the same row for N times, we still need to fill corresponding Field with null value.
    for (size_t grouping_offset = 0; grouping_offset < replicate_times_for_one_row; ++grouping_offset)
    {
        auto grouping_columns = getGroupSetColumnNamesByOffset(grouping_offset);
        // for every grouping col, get the mutated one of them.
        for (const auto & grouping_col : grouping_columns)
        {
            assert(block.getByName(grouping_col).column->isColumnNullable());

            const auto * nullable_column
                = typeid_cast<const ColumnNullable *>(block.getByName(grouping_col).column.get());
            auto cloned
                = ColumnNullable::create(nullable_column->getNestedColumnPtr(), nullable_column->getNullMapColumnPtr());
            auto * cloned_one = typeid_cast<ColumnNullable *>(cloned->assumeMutable().get());

            /// travel total rows, and set null values for current grouping set column.
            /// basically looks like:
            /// eg: source block         ==>        replicated block
            ///      <a, b>              ==>         <a, b, groupingID>  a new column is appended
            ///      1  1       target a -+----->     1  null  groupingID for a =1
            ///      2  2                 +----->     2  null  groupingID for b =2
            ///                 target b -+----->     null  1  groupingID for a =1
            ///                           +----->     null  2  groupingID for b =2
            ///
            /// after the replicate is now, the data form likes like below
            ///      <a, b, groupingID>              ==>       for one : in <a, b>
            ///    -----------------+                          locate the target row in every single small group with the same "offset_of_grouping_col" in set <a, b>
            ///      1  1       1   +  replicate_group1        for a, it's 0, we should pick and set:
            ///      1  1       2   +                              replicate_group_rows[0].a = null
            ///    -----------------+
            ///      2  2       1   +  replicate_group2        for b, it's 1, we should pick and set:
            ///      2  2       2   +                              replicate_group_rows[1].b = null
            ///    -----------------+
            for (size_t i = 0; i < origin_rows; ++i)
            {
                // for every original one row mapped N rows, fill the corresponding group set column as null value according to the offset.
                // only when the offset in replicate_group equals to current group_offset, set the data to null.
                // eg: for case above, for grouping_offset of <a> = 0, we only set the every offset = 0 in each
                // small replicate_group_x to null.
                for (UInt64 j = 0; j < replicate_times_for_one_row; ++j)
                {
                    if (j == grouping_offset)
                    {
                        // only keep this column value for targeted replica.
                        continue;
                    }
                    // set this column as null for all the other targeted replica.
                    // todo: since nullable column always be prior to computation of null value first, should we clean the old data at the same pos in nested column
                    auto computed_offset = i * replicate_times_for_one_row + j;
                    cloned_one->getNullMapData().data()[computed_offset] = 1;
                }
            }
            block.getByName(grouping_col).column = std::move(cloned);
        }
        // finish of adjustment for one grouping set columns. (by now one column for one grouping set).
    }
    block.insert(ColumnWithTypeAndName(
        std::move(grouping_id_column),
        grouping_identifier_column_type,
        grouping_identifier_column_name));
    // return input from block.
}

bool Expand::isInGroupSetColumn(String name) const
{
    return name_set.find(name) != name_set.end();
}

const GroupingColumnNames & Expand::getGroupSetColumnNamesByOffset(size_t offset) const
{
    /// currently, there only can be one groupingExprs in one groupingSet before the planner supporting the grouping set merge.
    return group_sets_names[offset][0];
}

const std::set<String> & Expand::getAllGroupSetColumnNames() const
{
    return name_set;
}

void Expand::collectNameSet()
{
    for (const auto & it1 : group_sets_names)
    {
        // for every grouping set.
        for (const auto & it2 : it1)
        {
            // for every grouping exprs
            for (const auto & it3 : it2)
            {
                name_set.insert(it3);
            }
        }
    }
}

const std::string Expand::grouping_identifier_column_name = "groupingID";
const DataTypePtr Expand::grouping_identifier_column_type = std::make_shared<DataTypeUInt64>();
} // namespace DB

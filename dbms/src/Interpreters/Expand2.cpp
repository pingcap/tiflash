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

#include <Interpreters/Expand2.h>

namespace DB
{

Expand2::Expand2(
    ExpressionActionsPtrVec projections_actions_,
    ExpressionActionsPtr before_expand_actions_,
    NamesWithAliasesVec projections_)
    : leveled_projections_actions(projections_actions_)
    , before_expand_actions(before_expand_actions_)
    , leveled_alias_projections(projections_)
{}

Block Expand2::next(const Block & block_cache, size_t i_th_project)
{
    /// step1: clone a new block
    ColumnsWithTypeAndName cloned_columns_and_type;
    auto num_columns = block_cache.columns();
    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        // todo: we can optimize it to just clone those columns that only needed/necessary in current level-projection.
        res[i] = block_cache.getColumns()[i]->cloneResized(block_cache.rows());
    }
    auto origin_column_and_types = block_cache.getColumnsWithTypeAndName();
    for (size_t i = 0; i < num_columns; ++i)
    {
        cloned_columns_and_type.emplace_back(
            std::move(res[i]),
            origin_column_and_types[i].type,
            origin_column_and_types[i].name,
            origin_column_and_types[i].column_id,
            origin_column_and_types[i].default_value);
    }
    Block cloned_block(cloned_columns_and_type);

    /// step2: execute a new block.
    auto ith_projection = leveled_projections_actions[i_th_project];
    ith_projection->execute(cloned_block);

    /// step3: organize a new block with unified alias name.
    Block new_block;
    auto names_with_alias = leveled_alias_projections[i_th_project];
    for (auto & one_alias : names_with_alias)
    {
        const std::string & name = one_alias.first;
        const std::string & alias = one_alias.second;
        ColumnWithTypeAndName column = cloned_block.getByName(name);
        if (!alias.empty())
            column.name = alias;
        new_block.insert(std::move(column));
    }

    /// step4: unfold the constant column which is not meaningful in global scope.
    for (size_t i = 0; i < new_block.getColumnsWithTypeAndName().size(); i++)
    {
        auto col = new_block.getColumnsWithTypeAndName()[i];
        if (col.column->isColumnConst())
        {
            // if it's a new literal constant column, unfold it, and if it's not an origin constant column, unfold it.
            // eg: grouping id projection(1) and grouping set column projection(null)
            if (!block_cache.getByName(col.name).column->isColumnConst())
                new_block.safeGetByPosition(i).column = col.column->convertToFullColumnIfConst();
        }
    }
    return new_block;
}

String Expand2::getLevelProjectionDes() const
{
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
        leveled_projections_actions.begin(),
        leveled_projections_actions.end(),
        [](const auto & item, FmtBuffer & buf) {
            // for every level-projection, make it as string too.
            buf.append("[");
            buf.joinStr(
                item->getActions().begin(),
                item->getActions().end(),
                [](const auto & item, FmtBuffer & buff) { buff.append(item.toString()); },
                ",");
            buf.append("]");
        },
        ";\n");
    buffer.append("]");
    return buffer.toString();
}

size_t Expand2::getLevelProjectionNum() const
{
    return leveled_projections_actions.size();
}

ExpressionActionsPtr & Expand2::getBeforeExpandActions()
{
    return before_expand_actions;
}
} // namespace DB

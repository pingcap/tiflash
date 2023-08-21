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

#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/ExpandBinder.h>

namespace DB::mock
{

bool ExpandBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExpand);
    tipb_executor->set_executor_id(name);
    tipb::Expand * expand = tipb_executor->mutable_expand();
    for (const auto & grouping_set : grouping_sets_columns)
    {
        auto * gss = expand->add_grouping_sets();
        for (const auto & grouping_exprs : grouping_set)
        {
            auto * ges = gss->add_grouping_exprs();
            for (const auto & grouping_col : grouping_exprs)
            {
                tipb::Expr * add_column = ges->add_grouping_expr();
                astToPB(
                    children[0]->output_schema,
                    grouping_col,
                    add_column,
                    collator_id,
                    context); // ast column ref change to tipb:Expr column ref
            }
        }
    }
    auto * children_executor = expand->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}

ExecutorBinderPtr compileExpand(
    ExecutorBinderPtr input,
    size_t & executor_index,
    MockVVecGroupingNameVec grouping_set_columns,
    std::set<String> in_set)
{
    DAGSchema output_schema;
    for (const auto & field : input->output_schema)
    {
        // if the column is in the grouping sets, make it nullable.
        if (in_set.find(field.first) != in_set.end() && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    {
        tipb::FieldType field_type{};
        field_type.set_tp(TiDB::TypeLongLong);
        field_type.set_charset("binary");
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull); // should have NOT NULL FLAG
        field_type.set_flen(-1);
        field_type.set_decimal(-1);
        output_schema.push_back(std::make_pair("groupingID", TiDB::fieldTypeToColumnInfo(field_type)));
    }
    ExecutorBinderPtr expand
        = std::make_shared<ExpandBinder>(executor_index, output_schema, std::move(grouping_set_columns));
    expand->children.push_back(input);
    return expand;
}
} // namespace DB::mock

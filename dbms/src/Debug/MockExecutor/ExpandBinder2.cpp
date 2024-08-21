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

#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/ExpandBinder2.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/IAST_fwd.h>


namespace DB::mock
{
bool ExpandBinder2::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExpand2);
    tipb_executor->set_executor_id(name);
    tipb::Expand2 * expand = tipb_executor->mutable_expand2();
    for (const auto & exprs : exprs_list)
    {
        auto * proj = expand->add_proj_exprs();
        for (size_t j = 0; j < exprs.size(); j++)
        {
            auto child = exprs[j];
            auto * proj_expr = proj->add_exprs();
            astToPB(children[0]->output_schema, child, proj_expr, collator_id, context);
            // since clickhouse ast literal(null)/literal(num) to tipb won't derive the right field-type inside (but DAG can), we make the injection here.
            auto * ft = proj_expr->mutable_field_type();
            ft->set_flag(fts[j].flag());
            ft->set_tp(fts[j].tp());
        }
    }
    // expand should fill the generated col output name in pb.
    for (const auto & one : output_names)
    {
        auto * mutable_output_name = expand->add_generated_output_names();
        *mutable_output_name = one;
    }
    auto * children_executor = expand->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}

ExecutorBinderPtr compileExpand2(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTs level_select_list,
    std::vector<String> output_names,
    std::vector<tipb::FieldType> fts)
{
    DAGSchema output_schema;
    std::vector<ASTs> expand_exprs;
    auto input_col_size = input->output_schema.size();
    for (size_t i = 0; i < level_select_list.size(); i++)
    {
        auto level_proj = level_select_list[i];
        ASTs level_exprs;
        for (size_t j = 0; j < level_proj->children.size(); j++)
        {
            auto expr = level_proj->children[j];
            level_exprs.push_back(expr);
            // for mock output schema, just output schema from the first level projection is adequate.
            if (i == 0)
            {
                auto ft
                    = std::find_if(input->output_schema.begin(), input->output_schema.end(), [&](const auto & field) {
                          return field.first == expr->getColumnName();
                      });
                auto output_name
                    = j < input_col_size ? input->output_schema[j].first : output_names[j - input_col_size];
                if (ft != input->output_schema.end())
                {
                    // base col ref (since ast can't derive the expression's field type, use the test injected one)
                    if (ft->second.hasNotNullFlag() && (fts[j].flag() & TiDB::ColumnFlagNotNull) == 0)
                    {
                        TiDB::ColumnInfo ci;
                        ci.flag = ft->second.flag;
                        ci.tp = ft->second.tp;
                        ci.name = ft->second.name;
                        ci.default_value = ft->second.default_value;
                        ci.collate = ft->second.collate;
                        ci.clearNotNullFlag();
                        output_schema.emplace_back(ft->first, ci);
                    }
                    else
                    {
                        output_schema.emplace_back(ft->first, ft->second);
                    }
                }
                else
                {
                    // expr: literal
                    auto ci = compileExpr(input->output_schema, expr);
                    // since ast literal itself can't derive the expression's field type and name, use the test injected one
                    ci.tp = static_cast<TiDB::TP>(fts[j].tp());
                    ci.flag = fts[j].flag();
                    output_schema.emplace_back(std::make_pair(output_name, ci));
                }
            }
        }
        expand_exprs.push_back(level_exprs);
    }
    ExecutorBinderPtr expand
        = std::make_shared<ExpandBinder2>(executor_index, output_schema, expand_exprs, output_names, fts);
    expand->children.push_back(input);
    return expand;
}
} // namespace DB::mock

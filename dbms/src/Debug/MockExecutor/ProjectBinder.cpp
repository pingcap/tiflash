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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/ProjectBinder.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>

namespace DB::mock
{
bool ProjectBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeProjection);
    tipb_executor->set_executor_id(name);
    auto * proj = tipb_executor->mutable_projection();
    auto & input_schema = children[0]->output_schema;
    for (const auto & child : exprs)
    {
        if (typeid_cast<ASTAsterisk *>(child.get()))
        {
            /// special case, select *
            for (size_t i = 0; i < input_schema.size(); ++i)
            {
                tipb::Expr * expr = proj->add_exprs();
                expr->set_tp(tipb::ColumnRef);
                *(expr->mutable_field_type()) = columnInfoToFieldType(input_schema[i].second);
                WriteBufferFromOwnString ss;
                encodeDAGInt64(i, ss);
                expr->set_val(ss.releaseStr());
            }
            continue;
        }
        tipb::Expr * expr = proj->add_exprs();
        astToPB(input_schema, child, expr, collator_id, context);
    }
    auto * children_executor = proj->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}

void ProjectBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    output_schema.erase(
        std::remove_if(
            output_schema.begin(),
            output_schema.end(),
            [&](const auto & field) { return used_columns.count(field.first) == 0; }),
        output_schema.end());
    std::unordered_set<String> used_input_columns;
    for (auto & expr : exprs)
    {
        if (typeid_cast<ASTAsterisk *>(expr.get()))
        {
            /// for select *, just add all its input columns, maybe
            /// can do some optimization, but it is not worth for mock
            /// tests
            for (auto & field : children[0]->output_schema)
            {
                used_input_columns.emplace(field.first);
            }
            break;
        }
        if (used_columns.find(expr->getColumnName()) != used_columns.end())
        {
            collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_input_columns);
        }
    }
    children[0]->columnPrune(used_input_columns);
}

ExecutorBinderPtr compileProject(ExecutorBinderPtr input, size_t & executor_index, ASTPtr select_list)
{
    std::vector<ASTPtr> exprs;
    DAGSchema output_schema;
    for (const auto & expr : select_list->children)
    {
        if (typeid_cast<ASTAsterisk *>(expr.get()))
        {
            /// special case, select *
            exprs.push_back(expr);
            const auto & last_output = input->output_schema;
            for (const auto & field : last_output)
            {
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(field.first, field.second);
            }
        }
        else
        {
            exprs.push_back(expr);
            auto ft = std::find_if(input->output_schema.begin(), input->output_schema.end(), [&](const auto & field) {
                return field.first == expr->getColumnName();
            });
            if (ft != input->output_schema.end())
            {
                output_schema.emplace_back(ft->first, ft->second);
                continue;
            }
            const auto * func = typeid_cast<const ASTFunction *>(expr.get());
            if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                throw Exception("No such agg " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
            }
            else
            {
                auto ci = compileExpr(input->output_schema, expr);
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(std::make_pair(expr->getColumnName(), ci));
            }
        }
    }
    auto project = std::make_shared<mock::ProjectBinder>(executor_index, output_schema, std::move(exprs));
    project->children.push_back(input);
    return project;
}
} // namespace DB::mock

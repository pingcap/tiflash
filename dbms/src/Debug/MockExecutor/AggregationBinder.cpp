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
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <Debug/MockExecutor/AggregationBinder.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/FuncSigMap.h>
#include <Parsers/ASTIdentifier.h>
#include <fmt/core.h>

namespace DB::mock
{
bool AggregationBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeAggregation);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);
    auto * agg = tipb_executor->mutable_aggregation();
    buildAggExpr(agg, collator_id, context);
    buildGroupBy(agg, collator_id, context);
    auto * child_executor = agg->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

void AggregationBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    /// output schema for partial agg is the original agg's output schema
    output_schema_for_partial_agg = output_schema;
    output_schema.erase(
        std::remove_if(
            output_schema.begin(),
            output_schema.end(),
            [&](const auto & field) { return used_columns.count(field.first) == 0; }),
        output_schema.end());
    std::unordered_set<String> used_input_columns;
    for (auto & func : agg_exprs)
    {
        if (used_columns.find(func->getColumnName()) != used_columns.end())
        {
            const auto * agg_func = typeid_cast<const ASTFunction *>(func.get());
            if (agg_func != nullptr)
            {
                /// agg_func should not be nullptr, just double check
                for (auto & child : agg_func->arguments->children)
                    collectUsedColumnsFromExpr(children[0]->output_schema, child, used_input_columns);
            }
        }
    }
    for (auto & gby_expr : gby_exprs)
    {
        collectUsedColumnsFromExpr(children[0]->output_schema, gby_expr, used_input_columns);
    }
    children[0]->columnPrune(used_input_columns);
}

void AggregationBinder::toMPPSubPlan(
    size_t & executor_index,
    const DAGProperties & properties,
    std::unordered_map<
        String,
        std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
{
    if (!is_final_mode)
    {
        children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
        return;
    }
    /// for aggregation, change aggregation to partial_aggregation => exchange_sender => exchange_receiver => final_aggregation
    // todo support avg
    if (has_uniq_raw_res)
        throw Exception("uniq raw res not supported in mpp query");
    std::shared_ptr<AggregationBinder> partial_agg = std::make_shared<AggregationBinder>(
        executor_index,
        output_schema_for_partial_agg,
        has_uniq_raw_res,
        false,
        std::move(agg_exprs),
        std::move(gby_exprs),
        false,
        fine_grained_shuffle_stream_count);
    partial_agg->children.push_back(children[0]);
    std::vector<size_t> partition_keys;
    size_t agg_func_num = partial_agg->agg_exprs.size();
    for (size_t i = 0; i < partial_agg->gby_exprs.size(); ++i)
    {
        partition_keys.push_back(i + agg_func_num);
    }

    std::shared_ptr<ExchangeSenderBinder> exchange_sender = std::make_shared<ExchangeSenderBinder>(
        executor_index,
        output_schema_for_partial_agg,
        partition_keys.empty() ? tipb::PassThrough : tipb::Hash,
        partition_keys,
        fine_grained_shuffle_stream_count);
    exchange_sender->children.push_back(partial_agg);

    std::shared_ptr<ExchangeReceiverBinder> exchange_receiver = std::make_shared<ExchangeReceiverBinder>(
        executor_index,
        output_schema_for_partial_agg,
        fine_grained_shuffle_stream_count);
    exchange_map[exchange_receiver->name] = std::make_pair(exchange_receiver, exchange_sender);

    /// re-construct agg_exprs and gby_exprs in final_agg
    for (size_t i = 0; i < partial_agg->agg_exprs.size(); ++i)
    {
        const auto * agg_func = typeid_cast<const ASTFunction *>(partial_agg->agg_exprs[i].get());
        ASTPtr update_agg_expr = agg_func->clone();
        auto * update_agg_func = typeid_cast<ASTFunction *>(update_agg_expr.get());
        if (agg_func->name == "count")
            update_agg_func->name = "sum";
        update_agg_func->arguments->children.clear();
        update_agg_func->arguments->children.push_back(
            std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[i].first));
        agg_exprs.push_back(update_agg_expr);
    }
    for (size_t i = 0; i < partial_agg->gby_exprs.size(); ++i)
    {
        gby_exprs.push_back(std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[agg_func_num + i].first));
    }
    children[0] = exchange_receiver;
}

bool AggregationBinder::needAppendProject() const
{
    return need_append_project;
}

size_t AggregationBinder::exprSize() const
{
    return agg_exprs.size() + gby_exprs.size();
}

bool AggregationBinder::hasUniqRawRes() const
{
    return has_uniq_raw_res;
}

void AggregationBinder::buildGroupBy(tipb::Aggregation * agg, int32_t collator_id, const Context & context) const
{
    auto & input_schema = children[0]->output_schema;
    for (const auto & child : gby_exprs)
    {
        tipb::Expr * gby = agg->add_group_by();
        astToPB(input_schema, child, gby, collator_id, context);
    }
}

void AggregationBinder::buildAggExpr(tipb::Aggregation * agg, int32_t collator_id, const Context & context) const
{
    auto & input_schema = children[0]->output_schema;

    for (const auto & expr : agg_exprs)
    {
        const auto * func = typeid_cast<const ASTFunction *>(expr.get());
        if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            throw Exception(
                "Only agg function is allowed in select for a query with aggregation",
                ErrorCodes::LOGICAL_ERROR);

        tipb::Expr * agg_func = agg->add_agg_func();

        for (const auto & arg : func->arguments->children)
        {
            tipb::Expr * arg_expr = agg_func->add_children();
            astToPB(input_schema, arg, arg_expr, collator_id, context);
        }

        buildAggFunc(agg_func, func, collator_id);
    }
}

void AggregationBinder::buildAggFunc(tipb::Expr * agg_func, const ASTFunction * func, int32_t collator_id) const
{
    auto agg_sig_it = tests::agg_func_name_to_sig.find(func->name);
    if (agg_sig_it == tests::agg_func_name_to_sig.end())
        throw Exception("Unsupported agg function: " + func->name, ErrorCodes::LOGICAL_ERROR);

    auto agg_sig = agg_sig_it->second;
    agg_func->set_tp(agg_sig);

    if (agg_sig == tipb::ExprType::Count || agg_sig == tipb::ExprType::Sum)
    {
        auto * ft = agg_func->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
    }
    else if (agg_sig == tipb::ExprType::Min || agg_sig == tipb::ExprType::Max || agg_sig == tipb::ExprType::First)
    {
        if (agg_func->children_size() != 1)
            throw Exception(fmt::format("Agg function({}) only accept 1 argument", func->name));

        auto * ft = agg_func->mutable_field_type();
        ft->set_tp(agg_func->children(0).field_type().tp());
        ft->set_decimal(agg_func->children(0).field_type().decimal());
        ft->set_flag(agg_func->children(0).field_type().flag() & (~TiDB::ColumnFlagNotNull));
        ft->set_collate(collator_id);
    }
    else if (agg_sig == tipb::ExprType::ApproxCountDistinct)
    {
        auto * ft = agg_func->mutable_field_type();
        ft->set_tp(TiDB::TypeString);
        ft->set_flag(1);
    }
    else if (agg_sig == tipb::ExprType::GroupConcat)
    {
        auto * ft = agg_func->mutable_field_type();
        ft->set_tp(TiDB::TypeString);
    }
    if (is_final_mode)
        agg_func->set_aggfuncmode(tipb::AggFunctionMode::FinalMode);
    else
        agg_func->set_aggfuncmode(tipb::AggFunctionMode::Partial1Mode);
}

ExecutorBinderPtr compileAggregation(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr agg_funcs,
    ASTPtr group_by_exprs,
    uint64_t fine_grained_shuffle_stream_count)
{
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    DAGSchema output_schema;
    bool has_uniq_raw_res = false;
    bool need_append_project = false;
    if (agg_funcs != nullptr)
    {
        for (const auto & expr : agg_funcs->children)
        {
            const auto * func = typeid_cast<const ASTFunction *>(expr.get());
            if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                need_append_project = true;
                continue;
            }

            agg_exprs.push_back(expr);
            std::vector<TiDB::ColumnInfo> children_ci;

            for (const auto & arg : func->arguments->children)
            {
                children_ci.push_back(compileExpr(input->output_schema, arg));
            }

            TiDB::ColumnInfo ci;
            if (func->name == "count")
            {
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull;
            }
            else if (
                func->name == "max" || func->name == "min" || func->name == "first_row" || func->name == "sum"
                || func->name == "avg")
            {
                ci = children_ci[0];
                ci.flag &= ~TiDB::ColumnFlagNotNull;
            }
            else if (func->name == uniq_raw_res_name)
            {
                has_uniq_raw_res = true;
                ci.tp = TiDB::TypeString;
                ci.flag = 1;
            }
            // TODO: Other agg func.
            else
            {
                throw Exception("Unsupported agg function: " + func->name, ErrorCodes::LOGICAL_ERROR);
            }

            output_schema.emplace_back(std::make_pair(func->getColumnName(), ci));
        }
    }

    if (group_by_exprs != nullptr)
    {
        for (const auto & child : group_by_exprs->children)
        {
            gby_exprs.push_back(child);
            auto ci = compileExpr(input->output_schema, child);
            output_schema.emplace_back(std::make_pair(child->getColumnName(), ci));
        }
    }

    auto aggregation = std::make_shared<mock::AggregationBinder>(
        executor_index,
        output_schema,
        has_uniq_raw_res,
        need_append_project,
        std::move(agg_exprs),
        std::move(gby_exprs),
        true,
        fine_grained_shuffle_stream_count);
    aggregation->children.push_back(input);
    return aggregation;
}
} // namespace DB::mock

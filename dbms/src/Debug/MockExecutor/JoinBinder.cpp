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
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/JoinBinder.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB::mock
{

void JoinBinder::addRuntimeFilter(MockRuntimeFilter & rf)
{
    rf_list.push_back(std::make_shared<MockRuntimeFilter>(rf));
}

void JoinBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    std::unordered_set<String> left_columns;
    std::unordered_set<String> right_columns;

    for (auto & field : children[0]->output_schema)
    {
        auto [db_name, table_name, column_name] = splitQualifiedName(field.first);
        left_columns.emplace(table_name + "." + column_name);
    }

    for (auto & field : children[1]->output_schema)
    {
        auto [db_name, table_name, column_name] = splitQualifiedName(field.first);
        right_columns.emplace(table_name + "." + column_name);
    }
    std::unordered_set<String> left_used_columns;
    std::unordered_set<String> right_used_columns;

    for (const auto & s : used_columns)
    {
        auto [db_name, table_name, col_name] = splitQualifiedName(s);
        auto t = table_name + "." + col_name;
        if (left_columns.find(t) != left_columns.end())
            left_used_columns.emplace(t);

        if (right_columns.find(t) != right_columns.end())
            right_used_columns.emplace(t);
    }

    for (const auto & child : join_cols)
    {
        if (auto * identifier = typeid_cast<ASTIdentifier *>(child.get()))
        {
            auto col_name = identifier->getColumnName();
            for (auto & field : children[0]->output_schema)
            {
                auto [db_name, table_name, column_name] = splitQualifiedName(field.first);
                if (col_name == column_name)
                {
                    left_used_columns.emplace(table_name + "." + column_name);
                    break;
                }
            }
            for (auto & field : children[1]->output_schema)
            {
                auto [db_name, table_name, column_name] = splitQualifiedName(field.first);
                if (col_name == column_name)
                {
                    right_used_columns.emplace(table_name + "." + column_name);
                    break;
                }
            }
        }
        else
        {
            throw Exception("Only support Join on columns");
        }
    }

    children[0]->columnPrune(left_used_columns);
    children[1]->columnPrune(right_used_columns);

    /// update output schema
    output_schema.clear();

    for (auto & field : children[0]->output_schema)
    {
        if (tp == tipb::TypeRightOuterJoin && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }

    for (auto & field : children[1]->output_schema)
    {
        if (tp == tipb::TypeLeftOuterJoin && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
}

void JoinBinder::fillJoinKeyAndFieldType(
    ASTPtr key,
    const DAGSchema & child_schema,
    tipb::Expr * tipb_key,
    tipb::FieldType * tipb_field_type,
    int32_t collator_id)
{
    auto * identifier = typeid_cast<ASTIdentifier *>(key.get());
    for (size_t index = 0; index < child_schema.size(); ++index)
    {
        const auto & [col_name, col_info] = child_schema[index];

        if (splitQualifiedName(col_name).column_name == identifier->getColumnName())
        {
            auto tipb_type = TiDB::columnInfoToFieldType(col_info);
            tipb_type.set_collate(collator_id);

            tipb_key->set_tp(tipb::ColumnRef);
            WriteBufferFromOwnString ss;
            encodeDAGInt64(index, ss);
            tipb_key->set_val(ss.releaseStr());
            *tipb_key->mutable_field_type() = tipb_type;

            *tipb_field_type = tipb_type;
            break;
        }
    }
}

bool JoinBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeJoin);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);

    tipb::Join * join = tipb_executor->mutable_join();

    join->set_join_type(tp);
    join->set_join_exec_type(tipb::JoinExecType::TypeHashJoin);
    join->set_inner_idx(inner_index);
    join->set_is_null_aware_semi_join(is_null_aware_semi_join);

    for (const auto & key : join_cols)
    {
        fillJoinKeyAndFieldType(
            key,
            children[0]->output_schema,
            join->add_left_join_keys(),
            join->add_probe_types(),
            collator_id);
        fillJoinKeyAndFieldType(
            key,
            children[1]->output_schema,
            join->add_right_join_keys(),
            join->add_build_types(),
            collator_id);
    }

    for (const auto & expr : left_conds)
    {
        tipb::Expr * cond = join->add_left_conditions();
        astToPB(children[0]->output_schema, expr, cond, collator_id, context);
    }

    for (const auto & expr : right_conds)
    {
        tipb::Expr * cond = join->add_right_conditions();
        astToPB(children[1]->output_schema, expr, cond, collator_id, context);
    }

    DAGSchema merged_children_schema{children[0]->output_schema};
    merged_children_schema.insert(
        merged_children_schema.end(),
        children[1]->output_schema.begin(),
        children[1]->output_schema.end());

    for (const auto & expr : other_conds)
    {
        tipb::Expr * cond = join->add_other_conditions();
        astToPB(merged_children_schema, expr, cond, collator_id, context);
    }

    for (const auto & expr : other_eq_conds_from_in)
    {
        tipb::Expr * cond = join->add_other_eq_conditions_from_in();
        astToPB(merged_children_schema, expr, cond, collator_id, context);
    }

    // add runtime filter
    for (const auto & rf : rf_list)
    {
        rf->toPB(
            children[1]->output_schema,
            children[0]->output_schema,
            collator_id,
            context,
            join->add_runtime_filter_list());
    }

    auto * left_child_executor = join->add_children();
    children[0]->toTiPBExecutor(left_child_executor, collator_id, mpp_info, context);
    auto * right_child_executor = join->add_children();
    return children[1]->toTiPBExecutor(right_child_executor, collator_id, mpp_info, context);
}

void JoinBinder::toMPPSubPlan(
    size_t & executor_index,
    const DAGProperties & properties,
    std::unordered_map<
        String,
        std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
{
    if (properties.use_broadcast_join)
    {
        /// for broadcast join, always use right side as the broadcast side
        std::shared_ptr<ExchangeSenderBinder> right_exchange_sender
            = std::make_shared<ExchangeSenderBinder>(executor_index, children[1]->output_schema, tipb::Broadcast);
        right_exchange_sender->children.push_back(children[1]);

        std::shared_ptr<ExchangeReceiverBinder> right_exchange_receiver
            = std::make_shared<ExchangeReceiverBinder>(executor_index, children[1]->output_schema);
        children[1] = right_exchange_receiver;
        exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
        return;
    }

    std::vector<size_t> left_partition_keys;
    std::vector<size_t> right_partition_keys;

    auto push_back_partition_key = [](auto & partition_keys, const auto & child_schema, const auto & key) {
        for (size_t index = 0; index < child_schema.size(); ++index)
        {
            if (splitQualifiedName(child_schema[index].first).column_name == key->getColumnName())
            {
                partition_keys.push_back(index);
                break;
            }
        }
    };

    for (const auto & key : join_cols)
    {
        push_back_partition_key(left_partition_keys, children[0]->output_schema, key);
        push_back_partition_key(right_partition_keys, children[1]->output_schema, key);
    }

    std::shared_ptr<ExchangeSenderBinder> left_exchange_sender = std::make_shared<ExchangeSenderBinder>(
        executor_index,
        children[0]->output_schema,
        tipb::Hash,
        left_partition_keys,
        fine_grained_shuffle_stream_count);
    left_exchange_sender->children.push_back(children[0]);
    std::shared_ptr<ExchangeSenderBinder> right_exchange_sender = std::make_shared<ExchangeSenderBinder>(
        executor_index,
        children[1]->output_schema,
        tipb::Hash,
        right_partition_keys,
        fine_grained_shuffle_stream_count);
    right_exchange_sender->children.push_back(children[1]);

    std::shared_ptr<ExchangeReceiverBinder> left_exchange_receiver = std::make_shared<ExchangeReceiverBinder>(
        executor_index,
        children[0]->output_schema,
        fine_grained_shuffle_stream_count);
    std::shared_ptr<ExchangeReceiverBinder> right_exchange_receiver = std::make_shared<ExchangeReceiverBinder>(
        executor_index,
        children[1]->output_schema,
        fine_grained_shuffle_stream_count);
    children[0] = left_exchange_receiver;
    children[1] = right_exchange_receiver;

    exchange_map[left_exchange_receiver->name] = std::make_pair(left_exchange_receiver, left_exchange_sender);
    exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
}

static void buildLeftSideJoinSchema(DAGSchema & schema, const DAGSchema & left_schema, tipb::JoinType tp)
{
    for (const auto & field : left_schema)
    {
        if (tp == tipb::JoinType::TypeRightOuterJoin && field.second.hasNotNullFlag())
            schema.push_back(toNullableDAGColumnInfo(field));
        else
            schema.push_back(field);
    }
}

static void buildRightSideJoinSchema(DAGSchema & schema, const DAGSchema & right_schema, tipb::JoinType tp)
{
    /// Note: for semi join, the right table column is ignored
    /// but for (anti) left outer semi join, a 1/0 (uint8) field is pushed back
    /// indicating whether right table has matching row(s), see comment in ASTTableJoin::Kind for details.
    if (tp == tipb::JoinType::TypeLeftOuterSemiJoin || tp == tipb::JoinType::TypeAntiLeftOuterSemiJoin)
    {
        tipb::FieldType field_type{};
        field_type.set_tp(TiDB::TypeTiny);
        field_type.set_charset("binary");
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_flag(0);
        field_type.set_flen(-1);
        field_type.set_decimal(-1);
        schema.push_back(std::make_pair("", TiDB::fieldTypeToColumnInfo(field_type)));
    }
    else if (tp != tipb::JoinType::TypeSemiJoin && tp != tipb::JoinType::TypeAntiSemiJoin)
    {
        for (const auto & field : right_schema)
        {
            if (tp == tipb::JoinType::TypeLeftOuterJoin && field.second.hasNotNullFlag())
                schema.push_back(toNullableDAGColumnInfo(field));
            else
                schema.push_back(field);
        }
    }
}

// compileJoin constructs a mocked Join executor node, note that all conditional expression params can be default
ExecutorBinderPtr compileJoin(
    size_t & executor_index,
    ExecutorBinderPtr left,
    ExecutorBinderPtr right,
    tipb::JoinType tp,
    const ASTs & join_cols,
    const ASTs & left_conds,
    const ASTs & right_conds,
    const ASTs & other_conds,
    const ASTs & other_eq_conds_from_in,
    uint64_t fine_grained_shuffle_stream_count,
    bool is_null_aware_semi_join,
    int64_t inner_index)
{
    DAGSchema output_schema;

    buildLeftSideJoinSchema(output_schema, left->output_schema, tp);
    buildRightSideJoinSchema(output_schema, right->output_schema, tp);

    auto join = std::make_shared<mock::JoinBinder>(
        executor_index,
        output_schema,
        tp,
        join_cols,
        left_conds,
        right_conds,
        other_conds,
        other_eq_conds_from_in,
        fine_grained_shuffle_stream_count,
        is_null_aware_semi_join,
        inner_index);
    join->children.push_back(left);
    join->children.push_back(right);

    return join;
}

/// Note: this api is only used by legacy test framework for compatibility purpose, which will be depracated soon,
/// so please avoid using it.
/// Old executor test framework bases on ch's parser to translate sql string to ast tree, then manually to DAGRequest.
/// However, as for join executor, this translation, from ASTTableJoin to tipb::Join, is not a one-to-one mapping
/// because of the different join classification model used by these two structures. Therefore, under old test framework,
/// it is hard to fully test join executor. New framework aims to directly construct DAGRequest, so new framework APIs for join should
/// avoid using ASTTableJoin.
ExecutorBinderPtr compileJoin(size_t & executor_index, ExecutorBinderPtr left, ExecutorBinderPtr right, ASTPtr params)
{
    tipb::JoinType tp;
    const auto & ast_join = (static_cast<const ASTTableJoin &>(*params));
    switch (ast_join.kind)
    {
    case ASTTableJoin::Kind::Inner:
        tp = tipb::JoinType::TypeInnerJoin;
        break;
    case ASTTableJoin::Kind::LeftOuter:
        tp = tipb::JoinType::TypeLeftOuterJoin;
        break;
    case ASTTableJoin::Kind::RightOuter:
        tp = tipb::JoinType::TypeRightOuterJoin;
        break;
    default:
        throw Exception("Unsupported join type");
    }

    // in legacy test framework, we only support using_expr of join
    ASTs join_cols;
    if (ast_join.using_expression_list)
    {
        for (const auto & key : ast_join.using_expression_list->children)
        {
            join_cols.push_back(key);
        }
    }
    return compileJoin(executor_index, left, right, tp, join_cols);
}
} // namespace DB::mock

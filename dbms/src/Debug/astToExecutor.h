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

#include <Debug/DAGProperties.h>
#include <Debug/DBGInvoker.h>
#include <Debug/MockTiDB.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Types.h>
#include <tipb/select.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NO_SUCH_COLUMN_IN_TABLE;
} // namespace ErrorCodes

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

namespace Debug
{
extern String LOCAL_HOST;
void setServiceAddr(const std::string & addr);
} // namespace Debug

std::pair<String, String> splitQualifiedName(const String & s);

struct MPPCtx
{
    Timestamp start_ts;
    Int64 next_task_id;
    std::vector<Int64> sender_target_task_ids;
    explicit MPPCtx(Timestamp start_ts_)
        : start_ts(start_ts_)
        , next_task_id(1)
    {}
};

using MPPCtxPtr = std::shared_ptr<MPPCtx>;

struct MPPInfo
{
    Timestamp start_ts;
    Int64 partition_id;
    Int64 task_id;
    const std::vector<Int64> sender_target_task_ids;
    const std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;

    MPPInfo(
        Timestamp start_ts_,
        Int64 partition_id_,
        Int64 task_id_,
        const std::vector<Int64> & sender_target_task_ids_,
        const std::unordered_map<String, std::vector<Int64>> & receiver_source_task_ids_map_)
        : start_ts(start_ts_)
        , partition_id(partition_id_)
        , task_id(task_id_)
        , sender_target_task_ids(sender_target_task_ids_)
        , receiver_source_task_ids_map(receiver_source_task_ids_map_)
    {}
};

struct TaskMeta
{
    UInt64 start_ts = 0;
    Int64 task_id = 0;
    Int64 partition_id = 0;
};

using TaskMetas = std::vector<TaskMeta>;

namespace mock
{
struct ExchangeSender;
struct ExchangeReceiver;
struct Executor
{
    size_t index;
    String name;
    DAGSchema output_schema;
    std::vector<std::shared_ptr<Executor>> children;
    virtual void columnPrune(std::unordered_set<String> & used_columns) = 0;
    Executor(size_t & index_, String && name_, const DAGSchema & output_schema_)
        : index(index_)
        , name(std::move(name_))
        , output_schema(output_schema_)
    {
        index_++;
    }
    virtual bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
        = 0;
    virtual void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map)
    {
        children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
    }
    virtual ~Executor() = default;
};

struct ExchangeSender : Executor
{
    tipb::ExchangeType type;
    TaskMetas task_metas;
    std::vector<size_t> partition_keys;
    ExchangeSender(size_t & index, const DAGSchema & output, tipb::ExchangeType type_, const std::vector<size_t> & partition_keys_ = {})
        : Executor(index, "exchange_sender_" + std::to_string(index), output)
        , type(type_)
        , partition_keys(partition_keys_)
    {}
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
};

struct ExchangeReceiver : Executor
{
    TaskMetas task_metas;
    ExchangeReceiver(size_t & index, const DAGSchema & output)
        : Executor(index, "exchange_receiver_" + std::to_string(index), output)
    {}
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context &) override;
};

struct TableScan : public Executor
{
    TableInfo table_info;
    /// used by column pruner
    TableScan(size_t & index_, const DAGSchema & output_schema_, const TableInfo & table_info_)
        : Executor(index_, "table_scan_" + std::to_string(index_), output_schema_)
        , table_info(table_info_)
    {}
    void columnPrune(std::unordered_set<String> & used_columns) override;
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t, const MPPInfo &, const Context &) override;
    void toMPPSubPlan(size_t &, const DAGProperties &, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> &) override
    {}

    void setTipbColumnInfo(tipb::ColumnInfo * ci, const DAGColumnInfo & dag_column_info) const
    {
        auto column_name = splitQualifiedName(dag_column_info.first).second;
        if (column_name == MutableSupport::tidb_pk_column_name)
            ci->set_column_id(-1);
        else
            ci->set_column_id(table_info.getColumnID(column_name));
        ci->set_tp(dag_column_info.second.tp);
        ci->set_flag(dag_column_info.second.flag);
        ci->set_columnlen(dag_column_info.second.flen);
        ci->set_decimal(dag_column_info.second.decimal);
        if (!dag_column_info.second.elems.empty())
        {
            for (const auto & pair : dag_column_info.second.elems)
            {
                ci->add_elems(pair.first);
            }
        }
    }
};

struct Selection : public Executor
{
    std::vector<ASTPtr> conditions;
    Selection(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> conditions_)
        : Executor(index_, "selection_" + std::to_string(index_), output_schema_)
        , conditions(std::move(conditions_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
    void columnPrune(std::unordered_set<String> & used_columns) override;
};

struct TopN : public Executor
{
    std::vector<ASTPtr> order_columns;
    size_t limit;
    TopN(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> order_columns_, size_t limit_)
        : Executor(index_, "topn_" + std::to_string(index_), output_schema_)
        , order_columns(std::move(order_columns_))
        , limit(limit_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
    void columnPrune(std::unordered_set<String> & used_columns) override;
};

struct Limit : public Executor
{
    size_t limit;
    Limit(size_t & index_, const DAGSchema & output_schema_, size_t limit_)
        : Executor(index_, "limit_" + std::to_string(index_), output_schema_)
        , limit(limit_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
    void columnPrune(std::unordered_set<String> & used_columns) override;
};

struct Aggregation : public Executor
{
    bool has_uniq_raw_res;
    bool need_append_project;
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    bool is_final_mode;
    DAGSchema output_schema_for_partial_agg;
    Aggregation(size_t & index_, const DAGSchema & output_schema_, bool has_uniq_raw_res_, bool need_append_project_, std::vector<ASTPtr> agg_exprs_, std::vector<ASTPtr> gby_exprs_, bool is_final_mode_)
        : Executor(index_, "aggregation_" + std::to_string(index_), output_schema_)
        , has_uniq_raw_res(has_uniq_raw_res_)
        , need_append_project(need_append_project_)
        , agg_exprs(std::move(agg_exprs_))
        , gby_exprs(std::move(gby_exprs_))
        , is_final_mode(is_final_mode_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
    void columnPrune(std::unordered_set<String> & used_columns) override;
    void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map) override;
};

struct Project : public Executor
{
    std::vector<ASTPtr> exprs;
    Project(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && exprs_)
        : Executor(index_, "project_" + std::to_string(index_), output_schema_)
        , exprs(std::move(exprs_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;
    void columnPrune(std::unordered_set<String> & used_columns) override;
};

struct Join : Executor
{
    ASTPtr params;
    const ASTTableJoin & join_params;
    Join(size_t & index_, const DAGSchema & output_schema_, ASTPtr params_)
        : Executor(index_, "Join_" + std::to_string(index_), output_schema_)
        , params(params_)
        , join_params(static_cast<const ASTTableJoin &>(*params))
    {
        if (join_params.using_expression_list == nullptr)
            throw Exception("No join condition found.");
        if (join_params.strictness != ASTTableJoin::Strictness::All)
            throw Exception("Only support join with strictness ALL");
    }

    void columnPrune(std::unordered_set<String> & used_columns) override;

    static void fillJoinKeyAndFieldType(
        ASTPtr key,
        const DAGSchema & schema,
        tipb::Expr * tipb_key,
        tipb::FieldType * tipb_field_type,
        uint32_t collator_id);

    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;

    void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties, std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map) override;
};
} // namespace mock

using ExecutorPtr = std::shared_ptr<mock::Executor>;

ExecutorPtr compileTableScan(size_t & executor_index, TableInfo & table_info, String & table_alias, bool append_pk_column);

ExecutorPtr compileSelection(ExecutorPtr input, size_t & executor_index, ASTPtr filter);

ExecutorPtr compileTopN(ExecutorPtr input, size_t & executor_index, ASTPtr order_exprs, ASTPtr limit_expr);

ExecutorPtr compileLimit(ExecutorPtr input, size_t & executor_index, ASTPtr limit_expr);

ExecutorPtr compileAggregation(ExecutorPtr input, size_t & executor_index, ASTPtr agg_funcs, ASTPtr group_by_exprs);

ExecutorPtr compileProject(ExecutorPtr input, size_t & executor_index, ASTPtr select_list);

ExecutorPtr compileJoin(size_t & executor_index, ExecutorPtr left, ExecutorPtr right, ASTPtr params);

ExecutorPtr compileExchangeSender(ExecutorPtr input, size_t & executor_index, tipb::ExchangeType exchange_type);

ExecutorPtr compileExchangeReceiver(size_t & executor_index, DAGSchema schema);

//TODO: add compileWindow

} // namespace DB
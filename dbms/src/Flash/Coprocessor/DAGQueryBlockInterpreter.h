#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class DAGQueryBlock;
class ExchangeReceiver;
class DAGExpressionAnalyzer;

/** build ch plan from dag request: dag executors -> ch plan
  */
class DAGQueryBlockInterpreter
{
public:
    DAGQueryBlockInterpreter(
        Context & context_,
        const std::vector<BlockInputStreams> & input_streams_vec_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        std::vector<SubqueriesForSets> & subqueries_for_sets_,
        const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

private:
    void executeRemoteQuery(DAGPipeline & pipeline);
    void executeImpl(DAGPipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline);
    void executeJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query);
    void prepareJoin(
        const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types,
        DAGPipeline & pipeline,
        Names & key_names,
        bool left,
        bool is_right_out_join,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);
    void executeExchangeReceiver(DAGPipeline & pipeline);
    void executeSourceProjection(DAGPipeline & pipeline, const tipb::Projection & projection);
    void executeExtraCastAndSelection(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & extra_cast,
        const NamesWithAliases & project_after_ts_and_filter_for_remote_read,
        const ExpressionActionsPtr & before_where,
        const ExpressionActionsPtr & project_after_where,
        const String & filter_column_name);
    ExpressionActionsPtr genJoinOtherConditionAction(
        const tipb::Join & join,
        std::vector<NameAndTypePair> & source_columns,
        String & filter_column_for_other_condition,
        String & filter_column_for_other_eq_condition);
    void executeWhere(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(DAGPipeline & pipeline, const std::vector<NameAndTypePair> & order_columns);
    void executeLimit(DAGPipeline & pipeline);
    void executeAggregation(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & expression_actions_ptr,
        Names & key_names,
        TiDB::TiDBCollators & collators,
        AggregateDescriptions & aggregate_descriptions);
    void executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols);
    void executeExchangeSender(DAGPipeline & pipeline);

    void recordProfileStreams(DAGPipeline & pipeline, const String & key);

    void recordJoinExecuteInfo(size_t build_side_index, const JoinPtr & join_ptr);

    void restorePipelineConcurrency(DAGPipeline & pipeline);

    void executeRemoteQueryImpl(
        DAGPipeline & pipeline,
        const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req,
        const DAGSchema & schema);

    DAGContext & dagContext() const { return *context.getDAGContext(); }
    const LogWithPrefixPtr & taskLogger() const { return dagContext().log; }

    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    TableLockHolder table_drop_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

    std::vector<const tipb::Expr *> conditions;
    std::vector<SubqueriesForSets> & subqueries_for_sets;
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map;
    std::vector<ExtraCastAfterTSMode> need_add_cast_column_flag_for_tablescan;

    LogWithPrefixPtr log;
};
} // namespace DB

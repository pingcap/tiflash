#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/TiDB.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{
class Context;

class DAGQuerySource;
class DAGQueryBlock;
struct RegionInfo;
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
        bool keep_session_timezone_info_,
        const DAGQuerySource & dag_,
        std::vector<SubqueriesForSets> & subqueries_for_sets_,
        const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map,
        const LogWithPrefixPtr & log_);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

    static void executeUnion(DAGPipeline & pipeline, size_t max_streams, const LogWithPrefixPtr & log);

private:
    void executeImpl(DAGPipeline & pipeline);
    void executeTableScan(
        DAGPipeline & pipeline,
        ExpressionActionsChain & chain,
        const tipb::TableScan & ts);
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
    ExpressionActionsPtr genJoinOtherConditionAction(
        const tipb::Join & join,
        std::vector<NameAndTypePair> & source_columns,
        String & filter_column_for_other_condition,
        String & filter_column_for_other_eq_condition);
    void executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(DAGPipeline & pipeline, const std::vector<NameAndTypePair> & order_columns);
    void executeLimit(DAGPipeline & pipeline);
    void executeAggregation(
        DAGPipeline & pipeline,
        ExpressionActionsChain & chain);
    void executeProject(DAGPipeline & pipeline, const NamesWithAliases & project_cols);

    void recordProfileStreams(DAGPipeline & pipeline, const String & key);

    void executeRemoteQueryImpl(
        DAGPipeline & pipeline,
        const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req,
        const DAGSchema & schema);

    void executeWhere(
        DAGPipeline & pipeline,
        ExpressionActionsChain & chain);

    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    const tipb::DAGRequest & rqst;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// How many streams before aggregation
    size_t before_agg_streams = 1;

    TableLockHolder table_drop_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

    std::vector<const tipb::Expr *> conditions;
    const DAGQuerySource & dag;
    std::vector<SubqueriesForSets> & subqueries_for_sets;
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map;
    std::vector<ExtraCastAfterTSMode> need_add_cast_column_flag_for_tablescan;

    const LogWithPrefixPtr log;
};
} // namespace DB

#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Mpp/getMPPTaskLog.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/TiDB.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <tipb/select.pb.h>

namespace DB
{
class Context;

class DAGQuerySource;
class DAGQueryBlock;
class ExchangeReceiver;
class DAGExpressionAnalyzer;

/** build ch plan from dag request: dag executors -> ch plan
  */
class TableScanInterpreter
{
public:
    TableScanInterpreter(
        Context & context_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const DAGQuerySource & dag_,
        const LogWithPrefixPtr & log_);

    ~TableScanInterpreter() = default;

    DAGPipelinePtr execute();

private:
    void executeRemoteQuery(DAGPipeline & pipeline);
    void executeImpl(DAGPipelinePtr & pipeline);
    void executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline);
    void executeWhere(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & expressionActionsPtr,
        const String & filter_column);
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

    void executeRemoteQueryImpl(
        DAGPipeline & pipeline,
        const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req,
        const DAGSchema & schema);

    Context & context;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    const tipb::DAGRequest & rqst;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// How many streams before aggregation
    size_t before_agg_streams = 1;

    TableLockHolder table_drop_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

    std::vector<const tipb::Expr *> conditions;
    const DAGQuerySource & dag;
    std::vector<ExtraCastAfterTSMode> need_add_cast_column_flag_for_tablescan;
    BoolVec is_remote_table_scan;

    const LogWithPrefixPtr log;
};
} // namespace DB

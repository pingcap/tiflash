// Copyright 2022 PingCAP, Ltd.
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

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
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
        std::vector<SubqueriesForSets> & subqueries_for_sets_);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

private:
    void executeImpl(DAGPipeline & pipeline);
    void handleTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline);
    void executeCastAfterTableScan(
        const TiDBTableScan & table_scan,
        const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
        size_t remote_read_streams_start_index,
        DAGPipeline & pipeline);
    void executePushedDownFilter(const std::vector<const tipb::Expr *> & conditions, size_t remote_read_streams_start_index, DAGPipeline & pipeline);
    void handleJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query);
    void prepareJoin(
        const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types,
        DAGPipeline & pipeline,
        Names & key_names,
        bool left,
        bool is_right_out_join,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);
    void handleExchangeReceiver(DAGPipeline & pipeline);
    void handleProjection(DAGPipeline & pipeline, const tipb::Projection & projection);
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
        AggregateDescriptions & aggregate_descriptions,
        bool is_final_agg);
    void executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols);
    void handleExchangeSender(DAGPipeline & pipeline);

    void recordProfileStreams(DAGPipeline & pipeline, const String & key);

    void recordJoinExecuteInfo(size_t build_side_index, const JoinPtr & join_ptr);

    void restorePipelineConcurrency(DAGPipeline & pipeline);

    void executeRemoteQueryImpl(
        DAGPipeline & pipeline,
        std::vector<RemoteRequest> & remote_requests);

    DAGContext & dagContext() const { return *context.getDAGContext(); }

    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

    std::vector<SubqueriesForSets> & subqueries_for_sets;

    LoggerPtr log;
};
} // namespace DB

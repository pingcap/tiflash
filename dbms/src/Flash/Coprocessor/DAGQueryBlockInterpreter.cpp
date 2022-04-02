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

#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/IOAdaptorBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

DAGQueryBlockInterpreter::DAGQueryBlockInterpreter(
    Context & context_,
    const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    std::vector<SubqueriesForSets> & subqueries_for_sets_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , keep_session_timezone_info(keep_session_timezone_info_)
    , max_streams(max_streams_)
    , subqueries_for_sets(subqueries_for_sets_)
    , log(Logger::get("DAGQueryBlockInterpreter", dagContext().log ? dagContext().log->identifier() : ""))
{}

namespace
{
struct AnalysisResult
{
    ExpressionActionsPtr before_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_having;
    ExpressionActionsPtr before_order_and_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    String having_column_name;
    std::vector<NameAndTypePair> order_columns;

    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    AggregateDescriptions aggregate_descriptions;
    bool is_final_agg;
};

// add timezone cast for timestamp type, this is used to support session level timezone
bool addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    ExpressionActionsChain & chain,
    const tipb::TableScan & table_scan)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
    {
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    }
    if (!has_need_cast_column)
        return false;
    return analyzer.appendExtraCastsAfterTS(chain, need_cast_column, table_scan);
}

bool isFinalAgg(const tipb::Expr & expr)
{
    if (!expr.has_aggfuncmode())
        /// set default value to true to make it compatible with old version of TiDB since before this
        /// change, all the aggregation in TiFlash is treated as final aggregation
        return true;
    return expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode || expr.aggfuncmode() == tipb::AggFunctionMode::CompleteMode;
}

AnalysisResult analyzeExpressions(
    Context & context,
    DAGExpressionAnalyzer & analyzer,
    const DAGQueryBlock & query_block,
    bool keep_session_timezone_info,
    NamesWithAliases & final_project)
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    // selection on table scan had been executed in handleTableScan
    if (query_block.selection && !query_block.isTableScanSource())
    {
        std::vector<const tipb::Expr *> where_conditions;
        for (const auto & c : query_block.selection->selection().conditions())
            where_conditions.push_back(&c);
        res.filter_column_name = analyzer.appendWhere(chain, where_conditions);
        res.before_where = chain.getLastActions();
        chain.addStep();
    }
    // There will be either Agg...
    if (query_block.aggregation)
    {
        /// set default value to true to make it compatible with old version of TiDB since before this
        /// change, all the aggregation in TiFlash is treated as final aggregation
        res.is_final_agg = true;
        const auto & aggregation = query_block.aggregation->aggregation();
        if (aggregation.agg_func_size() > 0 && !isFinalAgg(aggregation.agg_func(0)))
            res.is_final_agg = false;
        for (int i = 1; i < aggregation.agg_func_size(); i++)
        {
            if (res.is_final_agg != isFinalAgg(aggregation.agg_func(i)))
                throw TiFlashException("Different aggregation mode detected", Errors::Coprocessor::BadRequest);
        }
        // todo now we can tell if the aggregation is final stage or partial stage, maybe we can do collation insensitive
        //  aggregation if the stage is partial
        bool group_by_collation_sensitive =
            /// collation sensitive group by is slower than normal group by, use normal group by by default
            context.getSettingsRef().group_by_collation_sensitive || context.getDAGContext()->isMPPTask();

        std::tie(res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions, res.before_aggregation) = analyzer.appendAggregation(
            chain,
            query_block.aggregation->aggregation(),
            group_by_collation_sensitive);

        if (query_block.having != nullptr)
        {
            std::vector<const tipb::Expr *> having_conditions;
            for (const auto & c : query_block.having->selection().conditions())
                having_conditions.push_back(&c);
            res.having_column_name = analyzer.appendWhere(chain, having_conditions);
            res.before_having = chain.getLastActions();
            chain.addStep();
        }
    }
    // Or TopN, not both.
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::ExecType::TypeTopN)
    {
        res.order_columns = analyzer.appendOrderBy(chain, query_block.limit_or_topn->topn());
    }

    // Append final project results if needed.
    final_project = query_block.isRootQueryBlock()
        ? analyzer.appendFinalProjectForRootQueryBlock(
            chain,
            query_block.output_field_types,
            query_block.output_offsets,
            query_block.qb_column_prefix,
            keep_session_timezone_info)
        : analyzer.appendFinalProjectForNonRootQueryBlock(
            chain,
            query_block.qb_column_prefix);

    res.before_order_and_select = chain.getLastActions();

    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}

void setQuotaAndLimitsOnTableScan(Context & context, DAGPipeline & pipeline)
{
    const Settings & settings = context.getSettingsRef();

    IProfilingBlockInputStream::LocalLimits limits;
    limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
    limits.max_execution_time = settings.max_execution_time;
    limits.timeout_overflow_mode = settings.timeout_overflow_mode;

    /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
          *  because the initiating server has a summary of the execution of the request on all servers.
          *
          * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
          *  additionally on each remote server, because these limits are checked per block of data processed,
          *  and remote servers may process way more blocks of data than are received by initiator.
          */
    limits.min_execution_speed = settings.min_execution_speed;
    limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

    QuotaForIntervals & quota = context.getQuota();

    pipeline.transform([&](auto & stream) {
        if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
        {
            p_stream->setLimits(limits);
            p_stream->setQuota(quota);
        }
    });
}

} // namespace

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const Context & context,
    const NamesWithAliases & project_cols)
{
    auto columns = stream->getHeader();
    NamesAndTypesList input_column;
    for (const auto & column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(project_cols));
    return project;
}

void DAGQueryBlockInterpreter::handleTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline)
{
    bool has_region_to_read = false;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = dagContext().getTableRegionsInfoByTableID(physical_table_id);
        if (!table_regions_info.local_regions.empty() || !table_regions_info.remote_regions.empty())
        {
            has_region_to_read = true;
            break;
        }
    }
    if (!has_region_to_read)
        throw TiFlashException(fmt::format("Dag Request does not have region to read for table: {}", table_scan.getLogicalTableID()), Errors::Coprocessor::BadRequest);
    // construct pushed down filter conditions.
    std::vector<const tipb::Expr *> conditions;
    if (query_block.selection)
    {
        for (const auto & condition : query_block.selection->selection().conditions())
            conditions.push_back(&condition);
    }

    DAGStorageInterpreter storage_interpreter(context, query_block, table_scan, conditions, max_streams);
    storage_interpreter.execute(pipeline);

    analyzer = std::move(storage_interpreter.analyzer);


    auto remote_requests = std::move(storage_interpreter.remote_requests);
    auto null_stream_if_empty = std::move(storage_interpreter.null_stream_if_empty);

    // It is impossible to have no joined stream.
    assert(pipeline.streams_with_non_joined_data.empty());
    // after executeRemoteQueryImpl, remote read stream will be appended in pipeline.streams.
    size_t remote_read_streams_start_index = pipeline.streams.size();

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop / mpp mode.
    if (!remote_requests.empty())
        executeRemoteQueryImpl(pipeline, remote_requests);

    /// record local and remote io input stream
    auto & table_scan_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[query_block.source_name];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(null_stream_if_empty);
        // reset remote_read_streams_start_index for null_stream_if_empty.
        remote_read_streams_start_index = 1;
    }

    /// Theoretically we could move addTableLock to DAGStorageInterpreter, but we don't wants to the table to be dropped
    /// during the lifetime of this query, and sometimes if there is no local region, we will use the RemoteBlockInputStream
    /// or even the null_stream to hold the lock, so I would like too keep the addTableLock in DAGQueryBlockInterpreter
    pipeline.transform([&](auto & stream) {
        // todo do not need to hold all locks in each stream, if the stream is reading from table a
        //  it only needs to hold the lock of table a
        for (auto & lock : storage_interpreter.drop_locks)
        {
            stream->addTableLock(lock);
#ifdef TIFLASH_USE_FIBER
            stream = std::make_shared<IOAdaptorBlockInputStream>(5, stream, log->identifier());
#endif
        }
    });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);

    /// handle timezone/duration cast for local and remote table scan.
    executeCastAfterTableScan(storage_interpreter.is_need_add_cast_column, remote_read_streams_start_index, pipeline);
    recordProfileStreams(pipeline, query_block.source_name);

    /// handle pushed down filter for local and remote table scan.
    if (query_block.selection)
    {
        executePushedDownFilter(conditions, remote_read_streams_start_index, pipeline);
        recordProfileStreams(pipeline, query_block.selection_name);
    }
}

void DAGQueryBlockInterpreter::executePushedDownFilter(
    const std::vector<const tipb::Expr *> & conditions,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    ExpressionActionsChain chain;
    analyzer->initChain(chain, analyzer->getCurrentInputColumns());
    String filter_column_name = analyzer->appendWhere(chain, conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    NamesWithAliases project_cols;
    for (const auto & col : analyzer->getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    chain.getLastActions()->add(ExpressionAction::project(project_cols));
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    assert(pipeline.streams_with_non_joined_data.empty());
    assert(remote_read_streams_start_index <= pipeline.streams.size());
    // for remote read, filter had been pushed down, don't need to execute again.
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log->identifier());
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log->identifier());
    }
}

void DAGQueryBlockInterpreter::executeCastAfterTableScan(
    const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    auto original_source_columns = analyzer->getCurrentInputColumns();

    ExpressionActionsChain chain;
    analyzer->initChain(chain, original_source_columns);

    // execute timezone cast or duration cast if needed for local table scan
    if (addExtraCastsAfterTs(*analyzer, is_need_add_cast_column, chain, query_block.source->tbl_scan()))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();

        // After `addExtraCastsAfterTs`, analyzer->getCurrentInputColumns() has been modified.
        // For remote read, `timezone cast and duration cast` had been pushed down, don't need to execute cast expressions.
        // To keep the schema of local read streams and remote read streams the same, do project action for remote read streams.
        NamesWithAliases project_for_remote_read;
        const auto & after_cast_source_columns = analyzer->getCurrentInputColumns();
        for (size_t i = 0; i < after_cast_source_columns.size(); ++i)
        {
            project_for_remote_read.emplace_back(original_source_columns[i].name, after_cast_source_columns[i].name);
        }
        assert(!project_for_remote_read.empty());
        assert(pipeline.streams_with_non_joined_data.empty());
        assert(remote_read_streams_start_index <= pipeline.streams.size());
        size_t i = 0;
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
        }
        // remote streams
        if (i < pipeline.streams.size())
        {
            ExpressionActionsPtr project_for_cop_read = generateProjectExpressionActions(
                pipeline.streams[i],
                context,
                project_for_remote_read);
            while (i < pipeline.streams.size())
            {
                auto & stream = pipeline.streams[i++];
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, log->identifier());
            }
        }
    }
}

void DAGQueryBlockInterpreter::prepareJoin(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    DAGPipeline & pipeline,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name)
{
    std::vector<NameAndTypePair> source_columns;
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    if (dag_analyzer.appendJoinKeyAndJoinFilters(chain, keys, key_types, key_names, left, is_right_out_join, filters, filter_column_name))
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log->identifier()); });
    }
}

ExpressionActionsPtr DAGQueryBlockInterpreter::genJoinOtherConditionAction(
    const tipb::Join & join,
    std::vector<NameAndTypePair> & source_columns,
    String & filter_column_for_other_condition,
    String & filter_column_for_other_eq_condition)
{
    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0)
        return nullptr;
    DAGExpressionAnalyzer dag_analyzer(source_columns, context);
    ExpressionActionsChain chain;
    std::vector<const tipb::Expr *> condition_vector;
    if (join.other_conditions_size() > 0)
    {
        for (const auto & c : join.other_conditions())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }
    if (join.other_eq_conditions_from_in_size() > 0)
    {
        condition_vector.clear();
        for (const auto & c : join.other_eq_conditions_from_in())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_eq_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }
    return chain.getLastActions();
}

/// ClickHouse require join key to be exactly the same type
/// TiDB only require the join key to be the same category
/// for example decimal(10,2) join decimal(20,0) is allowed in
/// TiDB and will throw exception in ClickHouse
void getJoinKeyTypes(const tipb::Join & join, DataTypes & key_types)
{
    for (int i = 0; i < join.left_join_keys().size(); i++)
    {
        if (!exprHasValidFieldType(join.left_join_keys(i)) || !exprHasValidFieldType(join.right_join_keys(i)))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        DataTypes types;
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.left_join_keys(i).field_type()));
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.right_join_keys(i).field_type()));
        DataTypePtr common_type = getLeastSupertype(types);
        key_types.emplace_back(common_type);
    }
}

void DAGQueryBlockInterpreter::handleJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query)
{
    // build
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> equal_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::LeftAnti}};
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> cartesian_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Cross_Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Cross_Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Cross_Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftAnti}};

    if (input_streams_vec.size() != 2)
    {
        throw TiFlashException("Join query block must have 2 input streams", Errors::BroadcastJoin::Internal);
    }

    const auto & join_type_map = join.left_join_keys_size() == 0 ? cartesian_join_type_map : equal_join_type_map;
    auto join_type_it = join_type_map.find(join.join_type());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type in dag request", Errors::Coprocessor::BadRequest);

    /// (cartesian) (anti) left semi join.
    const bool is_left_semi_family = join.join_type() == tipb::JoinType::TypeLeftOuterSemiJoin || join.join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin;

    ASTTableJoin::Kind kind = join_type_it->second;
    const bool is_semi_join = join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin || is_left_semi_family;
    ASTTableJoin::Strictness strictness = ASTTableJoin::Strictness::All;
    if (is_semi_join)
        strictness = ASTTableJoin::Strictness::Any;

    /// in DAG request, inner part is the build side, however for TiFlash implementation,
    /// the build side must be the right side, so need to swap the join side if needed
    /// 1. for (cross) inner join, there is no problem in this swap.
    /// 2. for (cross) semi/anti-semi join, the build side is always right, needn't swap.
    /// 3. for non-cross left/right join, there is no problem in this swap.
    /// 4. for cross left join, the build side is always right, needn't and can't swap.
    /// 5. for cross right join, the build side is always left, so it will always swap and change to cross left join.
    /// note that whatever the build side is, we can't support cross-right join now.

    bool swap_join_side;
    if (kind == ASTTableJoin::Kind::Cross_Right)
        swap_join_side = true;
    else if (kind == ASTTableJoin::Kind::Cross_Left)
        swap_join_side = false;
    else
        swap_join_side = join.inner_idx() == 0;

    DAGPipeline left_pipeline;
    DAGPipeline right_pipeline;

    if (swap_join_side)
    {
        if (kind == ASTTableJoin::Kind::Left)
            kind = ASTTableJoin::Kind::Right;
        else if (kind == ASTTableJoin::Kind::Right)
            kind = ASTTableJoin::Kind::Left;
        else if (kind == ASTTableJoin::Kind::Cross_Right)
            kind = ASTTableJoin::Kind::Cross_Left;
        left_pipeline.streams = input_streams_vec[1];
        right_pipeline.streams = input_streams_vec[0];
    }
    else
    {
        left_pipeline.streams = input_streams_vec[0];
        right_pipeline.streams = input_streams_vec[1];
    }

    std::vector<NameAndTypePair> join_output_columns;
    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in input_streams_vec[0], then followed
    /// by the columns in input_streams_vec[1], if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names
    std::vector<NameAndTypePair> columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_other_join_filter;
    bool make_nullable = join.join_type() == tipb::JoinType::TypeRightOuterJoin;
    for (auto const & p : input_streams_vec[0][0]->getHeader().getNamesAndTypesList())
    {
        join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        column_set_for_other_join_filter.emplace(p.name);
    }
    make_nullable = join.join_type() == tipb::JoinType::TypeLeftOuterJoin;
    for (auto const & p : input_streams_vec[1][0]->getHeader().getNamesAndTypesList())
    {
        if (!is_semi_join)
            /// for semi join, the columns from right table will be ignored
            join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        /// however, when compiling join's other condition, we still need the columns from right table
        columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        column_set_for_other_join_filter.emplace(p.name);
    }

    bool is_tiflash_left_join = kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left;
    /// Cross_Right join will be converted to Cross_Left join, so no need to check Cross_Right
    bool is_tiflash_right_join = kind == ASTTableJoin::Kind::Right;
    /// all the columns from right table should be added after join, even for the join key
    NamesAndTypesList columns_added_by_join;
    make_nullable = is_tiflash_left_join;
    for (auto const & p : right_pipeline.streams[0]->getHeader().getNamesAndTypesList())
    {
        columns_added_by_join.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
    }

    String match_helper_name;
    if (is_left_semi_family)
    {
        const auto & left_block = input_streams_vec[0][0]->getHeader();
        const auto & right_block = input_streams_vec[1][0]->getHeader();

        match_helper_name = Join::match_helper_prefix;
        for (int i = 1; left_block.has(match_helper_name) || right_block.has(match_helper_name); ++i)
        {
            match_helper_name = Join::match_helper_prefix + std::to_string(i);
        }

        columns_added_by_join.emplace_back(match_helper_name, Join::match_helper_type);
        join_output_columns.emplace_back(match_helper_name, Join::match_helper_type);
    }

    DataTypes join_key_types;
    getJoinKeyTypes(join, join_key_types);
    TiDB::TiDBCollators collators;
    size_t join_key_size = join_key_types.size();
    if (join.probe_types_size() == static_cast<int>(join_key_size) && join.build_types_size() == join.probe_types_size())
        for (size_t i = 0; i < join_key_size; i++)
        {
            if (removeNullable(join_key_types[i])->isString())
            {
                if (join.probe_types(i).collate() != join.build_types(i).collate())
                    throw TiFlashException("Join with different collators on the join key", Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
                collators.push_back(nullptr);
        }

    Names left_key_names, right_key_names;
    String left_filter_column_name, right_filter_column_name;

    /// add necessary transformation if the join key is an expression

    prepareJoin(
        swap_join_side ? join.right_join_keys() : join.left_join_keys(),
        join_key_types,
        left_pipeline,
        left_key_names,
        true,
        is_tiflash_right_join,
        swap_join_side ? join.right_conditions() : join.left_conditions(),
        left_filter_column_name);

    prepareJoin(
        swap_join_side ? join.left_join_keys() : join.right_join_keys(),
        join_key_types,
        right_pipeline,
        right_key_names,
        false,
        is_tiflash_right_join,
        swap_join_side ? join.left_conditions() : join.right_conditions(),
        right_filter_column_name);

    String other_filter_column_name, other_eq_filter_from_in_column_name;
    for (auto const & p : left_pipeline.streams[0]->getHeader().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }
    for (auto const & p : right_pipeline.streams[0]->getHeader().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }

    ExpressionActionsPtr other_condition_expr
        = genJoinOtherConditionAction(join, columns_for_other_join_filter, other_filter_column_name, other_eq_filter_from_in_column_name);

    const Settings & settings = context.getSettingsRef();
    size_t join_build_concurrency = settings.join_concurrent_build ? std::min(max_streams, right_pipeline.streams.size()) : 1;
    size_t max_block_size_for_cross_join = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });

    JoinPtr join_ptr = std::make_shared<Join>(
        left_key_names,
        right_key_names,
        true,
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
        kind,
        strictness,
        log->identifier(),
        join_build_concurrency,
        collators,
        left_filter_column_name,
        right_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    recordJoinExecuteInfo(swap_join_side ? 0 : 1, join_ptr);

    // add a HashJoinBuildBlockInputStream to build a shared hash table
    size_t stream_index = 0;
    right_pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, stream_index++, log->identifier()); });
    executeUnion(right_pipeline, max_streams, log, /*ignore_block=*/true);

    right_query.source = right_pipeline.firstStream();
    right_query.join = join_ptr;
    right_query.join->setSampleBlock(right_query.source->getHeader());

    std::vector<NameAndTypePair> source_columns;
    for (const auto & p : left_pipeline.streams[0]->getHeader().getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.appendJoin(chain, right_query, columns_added_by_join);
    pipeline.streams = left_pipeline.streams;
    /// add join input stream
    if (is_tiflash_right_join)
    {
        auto & join_execute_info = dagContext().getJoinExecuteInfoMap()[query_block.source_name];
        for (size_t i = 0; i < join_build_concurrency; i++)
        {
            auto non_joined_stream = chain.getLastActions()->createStreamWithNonJoinedDataIfFullOrRightJoin(
                pipeline.firstStream()->getHeader(),
                i,
                join_build_concurrency,
                settings.max_block_size);
            pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    for (auto & stream : pipeline.streams)
        stream = std::make_shared<HashJoinProbeBlockInputStream>(stream, chain.getLastActions(), log->identifier());

    /// add a project to remove all the useless column
    NamesWithAliases project_cols;
    for (auto & c : join_output_columns)
    {
        /// do not need to care about duplicated column names because
        /// it is guaranteed by its children query block
        project_cols.emplace_back(c.name, c.name);
    }
    executeProject(pipeline, project_cols);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(join_output_columns), context);
}

void DAGQueryBlockInterpreter::recordJoinExecuteInfo(size_t build_side_index, const JoinPtr & join_ptr)
{
    const auto * build_side_root_executor = query_block.children[build_side_index]->root;
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_side_root_executor->executor_id();
    join_execute_info.join_ptr = join_ptr;
    assert(join_execute_info.join_ptr);
    dagContext().getJoinExecuteInfoMap()[query_block.source_name] = std::move(join_execute_info);
}

void DAGQueryBlockInterpreter::executeWhere(DAGPipeline & pipeline, const ExpressionActionsPtr & expr, String & filter_column)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column, log->identifier()); });
}

void DAGQueryBlockInterpreter::executeAggregation(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expression_actions_ptr,
    Names & key_names,
    TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expression_actions_ptr, log->identifier()); });

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
    {
        keys.push_back(header.getPositionByName(name));
    }
    for (auto & descr : aggregate_descriptions)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
            {
                descr.arguments.push_back(header.getPositionByName(name));
            }
        }
    }

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;
    bool has_collator = std::any_of(begin(collators), end(collators), [](const auto & p) { return p != nullptr; });

    Aggregator::Params params(
        header,
        keys,
        aggregate_descriptions,
        false,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by,
        !is_final_agg,
        context.getTemporaryPath(),
        has_collator ? collators : TiDB::dummy_collators);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, log);
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            stream_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            log->identifier());
        pipeline.streams.resize(1);
        // should record for agg before restore concurrency. See #3804.
        recordProfileStreams(pipeline, query_block.aggregation_name);
        restorePipelineConcurrency(pipeline);
    }
    else
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, log);
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);
        if (stream_with_non_joined_data)
            inputs.push_back(stream_with_non_joined_data);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs, log->identifier()),
            params,
            context.getFileProvider(),
            true,
            log->identifier());
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
    // add cast
}

void DAGQueryBlockInterpreter::executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr, log->identifier()); });
    }
}

void DAGQueryBlockInterpreter::executeOrder(DAGPipeline & pipeline, const std::vector<NameAndTypePair> & order_columns)
{
    SortDescription order_descr = getSortDescription(order_columns, query_block.limit_or_topn->topn().order_by());
    const Settings & settings = context.getSettingsRef();
    Int64 limit = query_block.limit_or_topn->topn().limit();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        log->identifier());
}

void DAGQueryBlockInterpreter::recordProfileStreams(DAGPipeline & pipeline, const String & key)
{
    auto & profile_streams = dagContext().getProfileStreamsMap()[key];
    pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
}

bool schemaMatch(const DAGSchema & left, const DAGSchema & right)
{
    if (left.size() != right.size())
        return false;
    for (size_t i = 0; i < left.size(); i++)
    {
        const auto & left_ci = left[i];
        const auto & right_ci = right[i];
        if (left_ci.second.tp != right_ci.second.tp)
            return false;
        if (left_ci.second.flag != right_ci.second.flag)
            return false;
    }
    return true;
}

void DAGQueryBlockInterpreter::executeRemoteQueryImpl(
    DAGPipeline & pipeline,
    std::vector<RemoteRequest> & remote_requests)
{
    assert(!remote_requests.empty());
    DAGSchema & schema = remote_requests[0].schema;
#ifndef NDEBUG
    for (size_t i = 1; i < remote_requests.size(); i++)
    {
        if (!schemaMatch(schema, remote_requests[i].schema))
            throw Exception("Schema mismatch between different partitions for partition table");
    }
#endif
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    std::vector<pingcap::coprocessor::copTask> all_tasks;
    for (const auto & remote_request : remote_requests)
    {
        pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
        remote_request.dag_request.SerializeToString(&(req->data));
        req->tp = pingcap::coprocessor::ReqType::DAG;
        req->start_ts = context.getSettingsRef().read_tso;
        req->schema_version = context.getSettingsRef().schema_version;

        pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
        pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
        auto tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, remote_request.key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"));
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }

    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    for (size_t i = 0, task_start = 0; i < concurrent_num; i++)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, log->identifier(), query_block.source_name);
#ifdef TIFLASH_USE_FIBER
        input = std::make_shared<IOAdaptorBlockInputStream>(5, input, log->identifier());
#endif
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void DAGQueryBlockInterpreter::handleExchangeReceiver(DAGPipeline & pipeline)
{
    auto it = dagContext().getMPPExchangeReceiverMap().find(query_block.source_name);
    if (unlikely(it == dagContext().getMPPExchangeReceiverMap().end()))
        throw Exception("Can not find exchange receiver for " + query_block.source_name, ErrorCodes::LOGICAL_ERROR);
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[query_block.source_name];
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(it->second, log->identifier(), query_block.source_name);
        exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, log->identifier());
        pipeline.streams.push_back(stream);
    }
    std::vector<NameAndTypePair> source_columns;
    Block block = pipeline.firstStream()->getHeader();
    for (const auto & col : block.getColumnsWithTypeAndName())
    {
        source_columns.emplace_back(NameAndTypePair(col.name, col.type));
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
}

void DAGQueryBlockInterpreter::handleProjection(DAGPipeline & pipeline, const tipb::Projection & projection)
{
    std::vector<NameAndTypePair> input_columns;
    pipeline.streams = input_streams_vec[0];
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(input_columns), context);
    ExpressionActionsChain chain;
    auto & last_step = dag_analyzer.initAndGetLastStep(chain);
    std::vector<NameAndTypePair> output_columns;
    NamesWithAliases project_cols;
    UniqueNameGenerator unique_name_generator;
    for (const auto & expr : projection.exprs())
    {
        auto expr_name = dag_analyzer.getActions(expr, last_step.actions);
        last_step.required_output.emplace_back(expr_name);
        const auto & col = last_step.actions->getSampleBlock().getByName(expr_name);
        String alias = unique_name_generator.toUniqueName(col.name);
        output_columns.emplace_back(alias, col.type);
        project_cols.emplace_back(col.name, alias);
    }
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log->identifier()); });
    executeProject(pipeline, project_cols);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has the following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void DAGQueryBlockInterpreter::executeImpl(DAGPipeline & pipeline)
{
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        SubqueryForSet right_query;
        handleJoin(query_block.source->join(), pipeline, right_query);
        recordProfileStreams(pipeline, query_block.source_name);

        SubqueriesForSets subquries;
        subquries[query_block.source_name] = right_query;
        subqueries_for_sets.emplace_back(subquries);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        handleExchangeReceiver(pipeline);
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeProjection)
    {
        handleProjection(pipeline, query_block.source->projection());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.isTableScanSource())
    {
        TiDBTableScan table_scan(query_block.source, dagContext());
        handleTableScan(table_scan, pipeline);
        dagContext().table_scan_executor_id = query_block.source_name;
    }
    else
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Unsupported source node: " + query_block.source_name,
            Errors::Coprocessor::BadRequest);
    }

    auto res = analyzeExpressions(
        context,
        *analyzer,
        query_block,
        keep_session_timezone_info,
        final_project);

    if (res.before_where)
    {
        // execute where
        executeWhere(pipeline, res.before_where, res.filter_column_name);
        recordProfileStreams(pipeline, query_block.selection_name);
    }

    // this log measures the concurrent degree in this mpp task
    LOG_FMT_DEBUG(
        log,
        "execution stream size for query block(before aggregation) {} is {}",
        query_block.qb_column_prefix,
        pipeline.streams.size());
    dagContext().final_concurrency = std::min(std::max(dagContext().final_concurrency, pipeline.streams.size()), max_streams);

    if (res.before_aggregation)
    {
        // execute aggregation
        executeAggregation(pipeline, res.before_aggregation, res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions, res.is_final_agg);
    }

    if (res.before_having)
    {
        // execute having
        executeWhere(pipeline, res.before_having, res.having_column_name);
        recordProfileStreams(pipeline, query_block.having_name);
    }

    if (res.before_order_and_select)
    {
        executeExpression(pipeline, res.before_order_and_select);
    }

    if (!res.order_columns.empty())
    {
        // execute topN
        executeOrder(pipeline, res.order_columns);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }

    // execute final project action
    executeProject(pipeline, final_project);

    // execute limit
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::TypeLimit)
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }

    restorePipelineConcurrency(pipeline);

    // execute exchange_sender
    if (query_block.exchange_sender)
    {
        handleExchangeSender(pipeline);
        recordProfileStreams(pipeline, query_block.exchange_sender_name);
    }
}

void DAGQueryBlockInterpreter::executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols)
{
    if (project_cols.empty())
        return;
    ExpressionActionsPtr project = generateProjectExpressionActions(pipeline.firstStream(), context, project_cols);
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project, log->identifier()); });
}

void DAGQueryBlockInterpreter::executeLimit(DAGPipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limit_or_topn->tp() == tipb::TypeLimit)
        limit = query_block.limit_or_topn->limit().limit();
    else
        limit = query_block.limit_or_topn->topn().limit();
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, log);
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    }
}

void DAGQueryBlockInterpreter::handleExchangeSender(DAGPipeline & pipeline)
{
    /// only run in MPP
    assert(dagContext().isMPPTask() && dagContext().tunnel_set != nullptr);
    /// exchange sender should be at the top of operators
    const auto & exchange_sender = query_block.exchange_sender->exchange_sender();
    /// get partition column ids
    const auto & part_keys = exchange_sender.partition_keys();
    std::vector<Int64> partition_col_id;
    TiDB::TiDBCollators collators;
    /// in case TiDB is an old version, it has no collation info
    bool has_collator_info = exchange_sender.types_size() != 0;
    if (has_collator_info && part_keys.size() != exchange_sender.types_size())
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB new collation is enabled",
            Errors::Coprocessor::BadRequest);
    }
    for (int i = 0; i < part_keys.size(); ++i)
    {
        const auto & expr = part_keys[i];
        assert(isColumnExpr(expr));
        auto column_index = decodeDAGInt64(expr.val());
        partition_col_id.emplace_back(column_index);
        if (has_collator_info && removeNullable(getDataTypeByFieldTypeForComputingLayer(expr.field_type()))->isString())
        {
            collators.emplace_back(getCollatorFromFieldType(exchange_sender.types(i)));
        }
        else
        {
            collators.emplace_back(nullptr);
        }
    }
    int stream_id = 0;
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(
            context.getDAGContext()->tunnel_set,
            partition_col_id,
            collators,
            exchange_sender.tp(),
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            stream_id++ == 0, /// only one stream needs to sending execution summaries for the last response
            dagContext());
        stream = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), log->identifier());
    });
}

void DAGQueryBlockInterpreter::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    restoreConcurrency(pipeline, dagContext().final_concurrency, log);
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    if (!pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(pipeline, max_streams, log);
        restorePipelineConcurrency(pipeline);
    }

    return pipeline.streams;
}
} // namespace DB

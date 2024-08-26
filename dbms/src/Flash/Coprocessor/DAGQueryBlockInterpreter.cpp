#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExchangeSender.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
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
    std::vector<SubqueriesForSets> & subqueries_for_sets_,
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , keep_session_timezone_info(keep_session_timezone_info_)
    , max_streams(max_streams_)
    , subqueries_for_sets(subqueries_for_sets_)
    , exchange_receiver_map(exchange_receiver_map_)
    , log(getMPPTaskLog(dagContext(), "DAGQueryBlockInterpreter"))
{
    if (query_block.selection != nullptr)
    {
        for (const auto & condition : query_block.selection->selection().conditions())
            conditions.push_back(&condition);
    }
}

namespace
{
struct AnalysisResult
{
    ExpressionActionsPtr extra_cast;
    NamesWithAliases project_after_ts_and_filter_for_remote_read;
    ExpressionActionsPtr before_where;
    ExpressionActionsPtr project_after_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_having;
    ExpressionActionsPtr before_order_and_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    String having_column_name;
    std::vector<NameAndTypePair> order_columns;
    /// Columns from the SELECT list, before renaming them to aliases.
    Names selected_columns;

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
    const DAGQueryBlock & query_block)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
    {
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    }
    if (!has_need_cast_column)
        return false;
    return analyzer.appendExtraCastsAfterTS(chain, need_cast_column, query_block);
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
    const std::vector<const tipb::Expr *> & conditions,
    const std::vector<ExtraCastAfterTSMode> & is_need_cast_column,
    bool keep_session_timezone_info,
    NamesWithAliases & final_project)
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    if (query_block.source->tp() == tipb::ExecType::TypeTableScan)
    {
        auto original_source_columns = analyzer.getCurrentInputColumns();
        if (addExtraCastsAfterTs(analyzer, is_need_cast_column, chain, query_block))
        {
            res.extra_cast = chain.getLastActions();
            chain.addStep();
            size_t index = 0;
            for (const auto & col : analyzer.getCurrentInputColumns())
            {
                res.project_after_ts_and_filter_for_remote_read.emplace_back(original_source_columns[index].name, col.name);
                ++index;
            }
        }
    }
    if (!conditions.empty())
    {
        res.filter_column_name = analyzer.appendWhere(chain, conditions);
        res.before_where = chain.getLastActions();
        chain.addStep();
        if (query_block.source->tp() == tipb::ExecType::TypeTableScan)
        {
            NamesWithAliases project_cols;
            for (const auto & col : analyzer.getCurrentInputColumns())
            {
                project_cols.emplace_back(col.name, col.name);
            }
            chain.getLastActions()->add(ExpressionAction::project(project_cols));
            res.project_after_where = chain.getLastActions();
            chain.addStep();
        }
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

        std::tie(res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions) = analyzer.appendAggregation(
            chain,
            query_block.aggregation->aggregation(),
            group_by_collation_sensitive);
        res.before_aggregation = chain.getLastActions();

        chain.finalize();
        chain.clear();

        // add cast if type is not match
        analyzer.appendAggSelect(chain, query_block.aggregation->aggregation());
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
    if (query_block.limitOrTopN && query_block.limitOrTopN->tp() == tipb::ExecType::TypeTopN)
    {
        res.order_columns = analyzer.appendOrderBy(chain, query_block.limitOrTopN->topn());
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

// the flow is the same as executeFetchcolumns
void DAGQueryBlockInterpreter::executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    if (dagContext().getRegionsForLocalRead().empty() && dagContext().getRegionsForRemoteRead().empty())
    {
        throw TiFlashException("Dag Request does not have region to read. ", Errors::Coprocessor::BadRequest);
    }

    DAGStorageInterpreter storage_interpreter(context, query_block, ts, conditions, max_streams);
    storage_interpreter.execute(pipeline);

    analyzer = std::move(storage_interpreter.analyzer);
    need_add_cast_column_flag_for_tablescan = std::move(storage_interpreter.is_need_add_cast_column);

    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    std::tie(std::ignore, table_drop_lock) = std::move(storage_interpreter.table_structure_lock).release();

    auto region_retry = std::move(storage_interpreter.region_retry);
    auto dag_req = std::move(storage_interpreter.dag_request);
    auto schema = std::move(storage_interpreter.dag_schema);
    auto null_stream_if_empty = std::move(storage_interpreter.null_stream_if_empty);

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop / mpp mode.
    if (!region_retry.empty())
    {
#ifndef NDEBUG
        if (unlikely(!dag_req.has_value() || !schema.has_value()))
            throw TiFlashException(
                "Try to read from remote but can not build DAG request. Should not happen!",
                Errors::Coprocessor::Internal);
#endif
        std::vector<pingcap::coprocessor::KeyRange> ranges;
        for (auto & info : region_retry)
        {
            for (const auto & range : info.get().key_ranges)
                ranges.emplace_back(*range.first, *range.second);
        }
        sort(ranges.begin(), ranges.end());
        executeRemoteQueryImpl(pipeline, ranges, *dag_req, *schema);
    }

    /// record local and remote io input stream
    auto & table_scan_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[query_block.source_name];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(null_stream_if_empty);
    }

    pipeline.transform([&](auto & stream) { stream->addTableLock(table_drop_lock); });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);
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
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), taskLogger()); });
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

void DAGQueryBlockInterpreter::executeJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query)
{
    // build
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> equal_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Anti}};
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> cartesian_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Cross_Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Cross_Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Cross_Anti}};
    if (input_streams_vec.size() != 2)
    {
        throw TiFlashException("Join query block must have 2 input streams", Errors::BroadcastJoin::Internal);
    }

    const auto & join_type_map = join.left_join_keys_size() == 0 ? cartesian_join_type_map : equal_join_type_map;
    auto join_type_it = join_type_map.find(join.join_type());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type in dag request", Errors::Coprocessor::BadRequest);

    ASTTableJoin::Kind kind = join_type_it->second;
    ASTTableJoin::Strictness strictness = ASTTableJoin::Strictness::All;
    bool is_semi_join = join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin;
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
        join_build_concurrency,
        collators,
        left_filter_column_name,
        right_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join);

    recordJoinExecuteInfo(swap_join_side ? 0 : 1, join_ptr);

    // add a HashJoinBuildBlockInputStream to build a shared hash table
    size_t stream_index = 0;
    right_pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, stream_index++, taskLogger()); });
    executeUnion(right_pipeline, max_streams, taskLogger(), /*ignore_block=*/true);

    right_query.source = right_pipeline.firstStream();
    right_query.join = join_ptr;
    right_query.join->setSampleBlock(right_query.source->getHeader());
    dagContext().getProfileStreamsMapForJoinBuildSide()[query_block.qb_join_subquery_alias].push_back(right_query.source);

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
<<<<<<< HEAD
            auto non_joined_stream = chain.getLastActions()->createStreamWithNonJoinedDataIfFullOrRightJoin(
=======
            auto non_joined_stream = createStreamWithNonJoinedRows(
                join_ptr,
>>>>>>> bf0d129d05 (avoid potential TiFlash crash in `NonJoinedBlockInputStream` (#9364))
                pipeline.firstStream()->getHeader(),
                i,
                join_build_concurrency,
                settings.max_block_size);
            pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    for (auto & stream : pipeline.streams)
        stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), taskLogger());

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
    dagContext().getJoinExecuteInfoMap()[query_block.source_name] = std::move(join_execute_info);
}

void DAGQueryBlockInterpreter::executeWhere(DAGPipeline & pipeline, const ExpressionActionsPtr & expr, String & filter_column)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column, taskLogger()); });
}

void DAGQueryBlockInterpreter::executeAggregation(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expression_actions_ptr,
    Names & key_names,
    TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expression_actions_ptr, taskLogger()); });

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
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, taskLogger());
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            stream_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            taskLogger());
        pipeline.streams.resize(1);
        // should record for agg before restore concurrency. See #3804.
        recordProfileStreams(pipeline, query_block.aggregation_name);
        restorePipelineConcurrency(pipeline);
    }
    else
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, taskLogger());
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);
        if (stream_with_non_joined_data)
            inputs.push_back(stream_with_non_joined_data);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs, taskLogger()),
            params,
            context.getFileProvider(),
            true,
            taskLogger());
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
    // add cast
}

void DAGQueryBlockInterpreter::executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr, taskLogger()); });
    }
}

void DAGQueryBlockInterpreter::executeOrder(DAGPipeline & pipeline, const std::vector<NameAndTypePair> & order_columns)
{
    SortDescription order_descr = getSortDescription(order_columns, query_block.limitOrTopN->topn().order_by());
    const Settings & settings = context.getSettingsRef();
    Int64 limit = query_block.limitOrTopN->topn().limit();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, taskLogger(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, taskLogger());

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        taskLogger());
}

void DAGQueryBlockInterpreter::recordProfileStreams(DAGPipeline & pipeline, const String & key)
{
    dagContext().getProfileStreamsMap()[key].qb_id = query_block.id;
    for (auto & stream : pipeline.streams)
    {
        dagContext().getProfileStreamsMap()[key].input_streams.push_back(stream);
    }
    for (auto & stream : pipeline.streams_with_non_joined_data)
        dagContext().getProfileStreamsMap()[key].input_streams.push_back(stream);
}

void copyExecutorTreeWithLocalTableScan(
    tipb::DAGRequest & dag_req,
    const tipb::Executor * root,
    const tipb::DAGRequest & org_req)
{
    const tipb::Executor * current = root;
    auto * exec = dag_req.mutable_root_executor();
    while (current->tp() != tipb::ExecType::TypeTableScan)
    {
        exec->set_tp(current->tp());
        exec->set_executor_id(current->executor_id());
        if (current->tp() == tipb::ExecType::TypeSelection)
        {
            auto * sel = exec->mutable_selection();
            for (auto const & condition : current->selection().conditions())
            {
                auto * tmp = sel->add_conditions();
                tmp->CopyFrom(condition);
            }
            exec = sel->mutable_child();
            current = &current->selection().child();
        }
        else if (current->tp() == tipb::ExecType::TypeAggregation || current->tp() == tipb::ExecType::TypeStreamAgg)
        {
            auto * agg = exec->mutable_aggregation();
            for (auto const & expr : current->aggregation().agg_func())
            {
                auto * tmp = agg->add_agg_func();
                tmp->CopyFrom(expr);
            }
            for (auto const & expr : current->aggregation().group_by())
            {
                auto * tmp = agg->add_group_by();
                tmp->CopyFrom(expr);
            }
            agg->set_streamed(current->aggregation().streamed());
            exec = agg->mutable_child();
            current = &current->aggregation().child();
        }
        else if (current->tp() == tipb::ExecType::TypeLimit)
        {
            auto * limit = exec->mutable_limit();
            limit->set_limit(current->limit().limit());
            exec = limit->mutable_child();
            current = &current->limit().child();
        }
        else if (current->tp() == tipb::ExecType::TypeTopN)
        {
            auto * topn = exec->mutable_topn();
            topn->set_limit(current->topn().limit());
            for (auto const & expr : current->topn().order_by())
            {
                auto * tmp = topn->add_order_by();
                tmp->CopyFrom(expr);
            }
            exec = topn->mutable_child();
            current = &current->topn().child();
        }
        else
        {
            throw TiFlashException("Not supported yet", Errors::Coprocessor::Unimplemented);
        }
    }

    if (current->tp() != tipb::ExecType::TypeTableScan)
        throw TiFlashException("Only support copy from table scan sourced query block", Errors::Coprocessor::Internal);
    exec->set_tp(tipb::ExecType::TypeTableScan);
    exec->set_executor_id(current->executor_id());
    auto * new_ts = new tipb::TableScan(current->tbl_scan());
    new_ts->set_next_read_engine(tipb::EngineType::Local);
    exec->set_allocated_tbl_scan(new_ts);

    /// force the encode type to be TypeCHBlock, so the receiver side does not need to handle the timezone related issues
    dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    dag_req.set_force_encode_type(true);
    if (org_req.has_time_zone_name() && !org_req.time_zone_name().empty())
        dag_req.set_time_zone_name(org_req.time_zone_name());
    else if (org_req.has_time_zone_offset())
        dag_req.set_time_zone_offset(org_req.time_zone_offset());
}

void DAGQueryBlockInterpreter::executeRemoteQuery(DAGPipeline & pipeline)
{
    // remote query containing agg/limit/topN can not running
    // in parellel, but current remote query is running in
    // parellel, so just disable this corner case.
    if (query_block.aggregation || query_block.limitOrTopN)
        throw TiFlashException("Remote query containing agg or limit or topN is not supported", Errors::Coprocessor::BadRequest);
    const auto & ts = query_block.source->tbl_scan();
    std::vector<pingcap::coprocessor::KeyRange> cop_key_ranges;
    cop_key_ranges.reserve(ts.ranges_size());
    for (const auto & range : ts.ranges())
    {
        cop_key_ranges.emplace_back(range.low(), range.high());
    }
    sort(cop_key_ranges.begin(), cop_key_ranges.end());

    ::tipb::DAGRequest dag_req;

    copyExecutorTreeWithLocalTableScan(dag_req, query_block.root, *dagContext().dag_request);
    DAGSchema schema;
    ColumnsWithTypeAndName columns;
    BoolVec is_ts_column;
    std::vector<NameAndTypePair> source_columns;
    for (int i = 0; i < static_cast<int>(query_block.output_field_types.size()); i++)
    {
        dag_req.add_output_offsets(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(query_block.output_field_types[i]);
        String col_name = query_block.qb_column_prefix + "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
        is_ts_column.push_back(query_block.output_field_types[i].tp() == TiDB::TypeTimestamp);
        source_columns.emplace_back(col_name, getDataTypeByFieldTypeForComputingLayer(query_block.output_field_types[i]));
        final_project.emplace_back(col_name, "");
    }

    dag_req.set_collect_execution_summaries(dagContext().collect_execution_summaries);
    executeRemoteQueryImpl(pipeline, cop_key_ranges, dag_req, schema);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
}

void DAGQueryBlockInterpreter::executeRemoteQueryImpl(
    DAGPipeline & pipeline,
    const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
    ::tipb::DAGRequest & dag_req,
    const DAGSchema & schema)
{
    pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
    dag_req.SerializeToString(&(req->data));
    req->tp = pingcap::coprocessor::ReqType::DAG;
    req->start_ts = context.getSettingsRef().read_tso;
    req->schema_version = context.getSettingsRef().schema_version;
    bool has_enforce_encode_type = dag_req.has_force_encode_type() && dag_req.force_encode_type();

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
    auto all_tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, cop_key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"));

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
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, taskLogger());
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void DAGQueryBlockInterpreter::executeExchangeReceiver(DAGPipeline & pipeline)
{
    auto it = exchange_receiver_map.find(query_block.source_name);
    if (unlikely(it == exchange_receiver_map.end()))
        throw Exception("Can not find exchange receiver for " + query_block.source_name, ErrorCodes::LOGICAL_ERROR);
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[query_block.source_name];
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(it->second, taskLogger());
        exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, taskLogger());
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

void DAGQueryBlockInterpreter::executeSourceProjection(DAGPipeline & pipeline, const tipb::Projection & projection)
{
    std::vector<NameAndTypePair> input_columns;
    pipeline.streams = input_streams_vec[0];
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(input_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.initChain(chain, dag_analyzer.getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();
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
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), taskLogger()); });
    executeProject(pipeline, project_cols);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
}

void DAGQueryBlockInterpreter::executeExtraCastAndSelection(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & extra_cast,
    const NamesWithAliases & project_after_ts_and_filter_for_remote_read,
    const ExpressionActionsPtr & before_where,
    const ExpressionActionsPtr & project_after_where,
    const String & filter_column_name)
{
    /// execute timezone cast and the selection
    ExpressionActionsPtr project_for_cop_read;
    for (auto & stream : pipeline.streams)
    {
        if (dynamic_cast<CoprocessorBlockInputStream *>(stream.get()) != nullptr)
        {
            /// for cop read, just execute the project is enough, because timezone cast and the selection are already done in remote TiFlash
            if (!project_after_ts_and_filter_for_remote_read.empty())
            {
                if (project_for_cop_read == nullptr)
                {
                    project_for_cop_read = generateProjectExpressionActions(stream, context, project_after_ts_and_filter_for_remote_read);
                }
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, taskLogger());
            }
        }
        else
        {
            /// execute timezone cast or duration cast if needed
            if (extra_cast)
                stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, taskLogger());
            /// execute selection if needed
            if (before_where)
            {
                stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, taskLogger());
                if (project_after_where)
                    stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, taskLogger());
            }
        }
    }
    for (auto & stream : pipeline.streams_with_non_joined_data)
    {
        /// execute selection if needed
        if (before_where)
        {
            stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, taskLogger());
            if (project_after_where)
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, taskLogger());
        }
    }
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void DAGQueryBlockInterpreter::executeImpl(DAGPipeline & pipeline)
{
    if (query_block.isRemoteQuery())
    {
        executeRemoteQuery(pipeline);
        return;
    }
    SubqueryForSet right_query;
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        executeJoin(query_block.source->join(), pipeline, right_query);
        recordProfileStreams(pipeline, query_block.source_name);

        SubqueriesForSets subquries;
        subquries[query_block.qb_join_subquery_alias] = right_query;
        subqueries_for_sets.emplace_back(subquries);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        executeExchangeReceiver(pipeline);
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeProjection)
    {
        executeSourceProjection(pipeline, query_block.source->projection());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else
    {
        executeTS(query_block.source->tbl_scan(), pipeline);
        recordProfileStreams(pipeline, query_block.source_name);
        dagContext().table_scan_executor_id = query_block.source_name;
    }

    auto res = analyzeExpressions(
        context,
        *analyzer,
        query_block,
        conditions,
        need_add_cast_column_flag_for_tablescan,
        keep_session_timezone_info,
        final_project);

    if (res.extra_cast || res.before_where)
    {
        executeExtraCastAndSelection(
            pipeline,
            res.extra_cast,
            res.project_after_ts_and_filter_for_remote_read,
            res.before_where,
            res.project_after_where,
            res.filter_column_name);
    }
    if (res.before_where)
    {
        recordProfileStreams(pipeline, query_block.selection_name);
    }

    // this log measures the concurrent degree in this mpp task
    LOG_FMT_DEBUG(
        log,
        "execution stream size for query block(before aggregation) {} is {}",
        query_block.qb_column_prefix,
        pipeline.streams.size());
    dagContext().final_concurrency = std::max(dagContext().final_concurrency, pipeline.streams.size());

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
        recordProfileStreams(pipeline, query_block.limitOrTopN_name);
    }

    // execute projection
    executeProject(pipeline, final_project);

    // execute limit
    if (query_block.limitOrTopN && query_block.limitOrTopN->tp() == tipb::TypeLimit)
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, query_block.limitOrTopN_name);
    }

    restorePipelineConcurrency(pipeline);

    // execute exchange_sender
    if (query_block.exchangeSender)
    {
        executeExchangeSender(pipeline);
        recordProfileStreams(pipeline, query_block.exchange_sender_name);
    }
}

void DAGQueryBlockInterpreter::executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols)
{
    if (project_cols.empty())
        return;
    ExpressionActionsPtr project = generateProjectExpressionActions(pipeline.firstStream(), context, project_cols);
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project, taskLogger()); });
}

void DAGQueryBlockInterpreter::executeLimit(DAGPipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limitOrTopN->tp() == tipb::TypeLimit)
        limit = query_block.limitOrTopN->limit().limit();
    else
        limit = query_block.limitOrTopN->topn().limit();
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, taskLogger(), false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, taskLogger());
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, taskLogger(), false); });
    }
}

void DAGQueryBlockInterpreter::executeExchangeSender(DAGPipeline & pipeline)
{
    /// only run in MPP
    assert(dagContext().isMPPTask() && dagContext().tunnel_set != nullptr);
    /// exchange sender should be at the top of operators
    const auto & exchange_sender = query_block.exchangeSender->exchange_sender();
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
            context.getSettings().dag_records_per_chunk,
            context.getSettings().batch_send_min_limit,
            stream_id++ == 0, /// only one stream needs to sending execution summaries for the last response
            dagContext());
        stream = std::make_shared<ExchangeSender>(stream, std::move(response_writer), taskLogger());
    });
}

void DAGQueryBlockInterpreter::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    restoreConcurrency(pipeline, dagContext().final_concurrency, taskLogger());
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    if (!pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(pipeline, max_streams, taskLogger());
        restorePipelineConcurrency(pipeline);
    }

    return pipeline.streams;
}
} // namespace DB

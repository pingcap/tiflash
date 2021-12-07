#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreter.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace FailPoints
{
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

JoinInterpreter::JoinInterpreter(
    Context & context_,
    const std::vector<DAGPipelinePtr> & input_pipelines_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    std::vector<SubqueriesForSets> & subqueries_for_sets_,
    const LogWithPrefixPtr & log_)
    : DAGInterpreterBase(
        context_,
        query_block_,
        max_streams_,
        keep_session_timezone_info_,
        log_)
    , input_pipelines(input_pipelines_)
    , subqueries_for_sets(subqueries_for_sets_)
{
    assert(query_block.source->tp() == tipb::ExecType::TypeJoin);
}

void JoinInterpreter::prepareJoin(
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
    DAGExpressionActionsChain chain;
    if (dag_analyzer.appendJoinKeyAndJoinFilters(chain, keys, key_types, key_names, left, is_right_out_join, filters, filter_column_name))
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log); });
    }
}

ExpressionActionsPtr JoinInterpreter::genJoinOtherConditionAction(
    const tipb::Join & join,
    std::vector<NameAndTypePair> & source_columns,
    String & filter_column_for_other_condition,
    String & filter_column_for_other_eq_condition)
{
    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0)
        return nullptr;
    DAGExpressionAnalyzer dag_analyzer(source_columns, context);
    DAGExpressionActionsChain chain;
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
static void getJoinKeyTypes(const tipb::Join & join, DataTypes & key_types)
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

void JoinInterpreter::executeJoin(const tipb::Join & join, DAGPipelinePtr & pipeline, SubqueryForSet & right_query)
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
    if (input_pipelines.size() != 2)
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

    DAGPipelinePtr left_pipeline;
    DAGPipelinePtr right_pipeline;

    if (swap_join_side)
    {
        if (kind == ASTTableJoin::Kind::Left)
            kind = ASTTableJoin::Kind::Right;
        else if (kind == ASTTableJoin::Kind::Right)
            kind = ASTTableJoin::Kind::Left;
        else if (kind == ASTTableJoin::Kind::Cross_Right)
            kind = ASTTableJoin::Kind::Cross_Left;
        left_pipeline = input_pipelines[1];
        right_pipeline = input_pipelines[0];
    }
    else
    {
        left_pipeline = input_pipelines[0];
        right_pipeline = input_pipelines[1];
    }

    std::vector<NameAndTypePair> join_output_columns;
    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in input_pipelines[0], then followed
    /// by the columns in input_pipelines[1], if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names
    std::vector<NameAndTypePair> columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_other_join_filter;
    bool make_nullable = join.join_type() == tipb::JoinType::TypeRightOuterJoin;
    for (auto const & p : input_pipelines[0]->firstStream()->getHeader().getNamesAndTypesList())
    {
        join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        column_set_for_other_join_filter.emplace(p.name);
    }
    make_nullable = join.join_type() == tipb::JoinType::TypeLeftOuterJoin;
    for (auto const & p : input_pipelines[1]->firstStream()->getHeader().getNamesAndTypesList())
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
    for (auto const & p : right_pipeline->firstStream()->getHeader().getNamesAndTypesList())
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
        *left_pipeline,
        left_key_names,
        true,
        is_tiflash_right_join,
        swap_join_side ? join.right_conditions() : join.left_conditions(),
        left_filter_column_name);

    prepareJoin(
        swap_join_side ? join.left_join_keys() : join.right_join_keys(),
        join_key_types,
        *right_pipeline,
        right_key_names,
        false,
        is_tiflash_right_join,
        swap_join_side ? join.left_conditions() : join.right_conditions(),
        right_filter_column_name);

    String other_filter_column_name, other_eq_filter_from_in_column_name;
    for (auto const & p : left_pipeline->firstStream()->getHeader().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }
    for (auto const & p : right_pipeline->firstStream()->getHeader().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }

    ExpressionActionsPtr other_condition_expr
        = genJoinOtherConditionAction(join, columns_for_other_join_filter, other_filter_column_name, other_eq_filter_from_in_column_name);

    const Settings & settings = context.getSettingsRef();
    size_t join_build_concurrency = settings.join_concurrent_build ? std::min(max_streams, right_pipeline->streams.size()) : 1;
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

    // add a HashJoinBuildBlockInputStream to build a shared hash table
    size_t stream_index = 0;
    right_pipeline->transform(
        [&](auto & stream) { stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, stream_index++, log); });
    executeUnion(*right_pipeline, max_streams, log);

    right_query.source = right_pipeline->firstStream();
    right_query.join = join_ptr;
    right_query.join->setSampleBlock(right_query.source->getHeader());
    dagContext().getProfileStreamsMapForJoinBuildSide()[query_block.qb_join_subquery_alias].push_back(right_query.source);

    std::vector<NameAndTypePair> source_columns;
    for (const auto & p : left_pipeline->firstStream()->getHeader().getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    DAGExpressionActionsChain chain; // Do not use pipeline->chain
    dag_analyzer.appendJoin(chain, right_query, columns_added_by_join);
    pipeline = left_pipeline;
    /// add join input stream
    if (is_tiflash_right_join)
    {
        for (size_t i = 0; i < join_build_concurrency; i++)
            pipeline->streams_with_non_joined_data.push_back(chain.getLastActions()->createStreamWithNonJoinedDataIfFullOrRightJoin(
                pipeline->firstStream()->getHeader(),
                i,
                join_build_concurrency,
                settings.max_block_size));
    }
    for (auto & stream : pipeline->streams)
        stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log);

    /// add a project to remove all the useless column
    NamesWithAliases project_cols;
    for (auto & c : join_output_columns)
    {
        /// do not need to care about duplicated column names because
        /// it is guaranteed by its children query block
        project_cols.emplace_back(c.name, c.name);
    }
    executeProject(*pipeline, project_cols);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(join_output_columns), context);
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
void JoinInterpreter::executeImpl(DAGPipelinePtr & pipeline)
{
    SubqueryForSet right_query;
    executeJoin(query_block.source->join(), pipeline, right_query);
    recordProfileStreams(dagContext(), *pipeline, query_block.source_name, query_block.id);

    SubqueriesForSets subquries;
    subquries[query_block.qb_join_subquery_alias] = right_query;
    subqueries_for_sets.emplace_back(subquries);
}

} // namespace DB

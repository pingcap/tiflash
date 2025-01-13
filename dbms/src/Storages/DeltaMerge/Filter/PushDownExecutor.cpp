// Copyright 2024 PingCAP, Inc.
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

#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Filter/PushDownExecutor.h>
#include <Storages/SelectQueryInfo.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB::DM
{
PushDownExecutorPtr PushDownExecutor::build(
    const RSOperatorPtr & rs_operator,
    const ANNQueryInfoPtr & ann_query_info,
    const TiDB::ColumnInfos & table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
    const ColumnDefines & columns_to_read,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    // check if the ann_query_info is valid
    auto valid_ann_query_info = ann_query_info;
    if (ann_query_info)
    {
        bool is_valid_ann_query = ann_query_info->top_k() != std::numeric_limits<UInt32>::max();
        bool is_matching_ann_query = std::any_of(
            columns_to_read.begin(),
            columns_to_read.end(),
            [cid = ann_query_info->column_id()](const ColumnDefine & cd) -> bool { return cd.id == cid; });
        if (!is_valid_ann_query || !is_matching_ann_query)
            valid_ann_query_info = nullptr;
    }

    if (pushed_down_filters.empty())
    {
        LOG_DEBUG(tracing_logger, "Push down filter is empty");
        return std::make_shared<PushDownExecutor>(rs_operator, valid_ann_query_info);
    }
    std::unordered_map<ColumnID, ColumnDefine> columns_to_read_map;
    for (const auto & column : columns_to_read)
        columns_to_read_map.emplace(column.id, column);

    // Get the columns of the filter, is a subset of columns_to_read
    std::unordered_set<ColumnID> filter_col_id_set;
    for (const auto & expr : pushed_down_filters)
    {
        getColumnIDsFromExpr(expr, table_scan_column_info, filter_col_id_set);
    }
    auto filter_columns = std::make_shared<DM::ColumnDefines>();
    filter_columns->reserve(filter_col_id_set.size());
    for (const auto & cid : filter_col_id_set)
    {
        RUNTIME_CHECK_MSG(
            columns_to_read_map.contains(cid),
            "Filter ColumnID({}) not found in columns_to_read_map",
            cid);
        filter_columns->emplace_back(columns_to_read_map.at(cid));
    }

    // The source_columns_of_analyzer should be the same as the size of table_scan_column_info
    // The columns_to_read is a subset of table_scan_column_info, when there are generated columns and extra table id column.
    NamesAndTypes source_columns_of_analyzer;
    source_columns_of_analyzer.reserve(table_scan_column_info.size());
    for (size_t i = 0; i < table_scan_column_info.size(); ++i)
    {
        auto const & ci = table_scan_column_info[i];
        const auto cid = ci.id;
        if (ci.hasGeneratedColumnFlag())
        {
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(ci);
            source_columns_of_analyzer.emplace_back(col_name, data_type);
            continue;
        }
        if (cid == MutSup::extra_table_id_col_id)
        {
            source_columns_of_analyzer.emplace_back(
                MutSup::extra_table_id_column_name,
                MutSup::getExtraTableIdColumnType());
            continue;
        }
        RUNTIME_CHECK_MSG(columns_to_read_map.contains(cid), "ColumnID({}) not found in columns_to_read_map", cid);
        source_columns_of_analyzer.emplace_back(columns_to_read_map.at(cid).name, columns_to_read_map.at(cid).type);
    }
    auto analyzer = std::make_unique<DAGExpressionAnalyzer>(source_columns_of_analyzer, context);

    // Build the extra cast
    ExpressionActionsPtr extra_cast = nullptr;
    // need_cast_column should be the same size as table_scan_column_info and source_columns_of_analyzer
    std::vector<UInt8> may_need_add_cast_column;
    may_need_add_cast_column.reserve(table_scan_column_info.size());
    for (const auto & col : table_scan_column_info)
        may_need_add_cast_column.push_back(
            !col.hasGeneratedColumnFlag() && filter_col_id_set.contains(col.id) && col.id != -1);
    ExpressionActionsChain chain;
    auto & step = analyzer->initAndGetLastStep(chain);
    auto & actions = step.actions;
    if (auto [has_cast, casted_columns]
        = analyzer->buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan_column_info);
        has_cast)
    {
        NamesWithAliases project_cols;
        for (size_t i = 0; i < table_scan_column_info.size(); ++i)
        {
            if (filter_col_id_set.contains(table_scan_column_info[i].id))
            {
                auto it = columns_to_read_map.find(table_scan_column_info[i].id);
                RUNTIME_CHECK(it != columns_to_read_map.end(), table_scan_column_info[i].id);
                project_cols.emplace_back(casted_columns[i], it->second.name);
            }
        }
        actions->add(ExpressionAction::project(project_cols));

        for (const auto & col : *filter_columns)
            step.required_output.push_back(col.name);

        extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();
        LOG_DEBUG(tracing_logger, "Extra cast for filter columns: {}", extra_cast->dumpActions());
    }

    // build filter expression actions
    auto [before_where, filter_column_name, project_after_where] = analyzer->buildPushDownExecutor(pushed_down_filters);
    LOG_DEBUG(tracing_logger, "Push down filter: {}", before_where->dumpActions());

    // record current column defines
    auto columns_after_cast = std::make_shared<ColumnDefines>();
    if (extra_cast != nullptr)
    {
        columns_after_cast->reserve(columns_to_read.size());
        const auto & current_names_and_types = analyzer->getCurrentInputColumns();
        for (size_t i = 0; i < table_scan_column_info.size(); ++i)
        {
            if (table_scan_column_info[i].hasGeneratedColumnFlag()
                || table_scan_column_info[i].id == MutSup::extra_table_id_col_id)
                continue;
            auto col = columns_to_read_map.at(table_scan_column_info[i].id);
            RUNTIME_CHECK_MSG(
                col.name == current_names_and_types[i].name,
                "Column name mismatch, expect: {}, actual: {}",
                col.name,
                current_names_and_types[i].name);
            columns_after_cast->push_back(col);
            columns_after_cast->back().type = current_names_and_types[i].type;
        }
    }

    return std::make_shared<PushDownExecutor>(
        rs_operator,
        valid_ann_query_info,
        before_where,
        project_after_where,
        filter_columns,
        filter_column_name,
        extra_cast,
        columns_after_cast);
}

PushDownExecutorPtr PushDownExecutor::build(
    const SelectQueryInfo & query_info,
    const ColumnDefines & columns_to_read,
    const ColumnDefines & table_column_defines,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    const auto & dag_query = query_info.dag_query;
    if (unlikely(dag_query == nullptr))
        return EMPTY_FILTER;

    const auto & columns_to_read_info = dag_query->source_columns;
    // build rough set operator
    const auto rs_operator = RSOperator::build(
        dag_query,
        columns_to_read_info,
        table_column_defines,
        context.getSettingsRef().dt_enable_rough_set_filter,
        tracing_logger);
    // build ann_query_info
    ANNQueryInfoPtr ann_query_info = nullptr;
    if (dag_query->ann_query_info.query_type() != tipb::ANNQueryType::InvalidQueryType)
        ann_query_info = std::make_shared<tipb::ANNQueryInfo>(dag_query->ann_query_info);
    // build push down filter
    const auto & pushed_down_filters = dag_query->pushed_down_filters;
    if (unlikely(context.getSettingsRef().force_push_down_all_filters_to_scan) && !dag_query->filters.empty())
    {
        google::protobuf::RepeatedPtrField<tipb::Expr> merged_filters{
            pushed_down_filters.begin(),
            pushed_down_filters.end()};
        merged_filters.MergeFrom(dag_query->filters);
        return PushDownExecutor::build(
            rs_operator,
            ann_query_info,
            columns_to_read_info,
            merged_filters,
            columns_to_read,
            context,
            tracing_logger);
    }
    return PushDownExecutor::build(
        rs_operator,
        ann_query_info,
        columns_to_read_info,
        pushed_down_filters,
        columns_to_read,
        context,
        tracing_logger);
}
} // namespace DB::DM

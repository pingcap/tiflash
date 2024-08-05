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

#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/SelectQueryInfo.h>
#include <TiDB/Decode/TypeMapping.h>

#include <magic_enum.hpp>

namespace DB::DM
{
namespace
{
const ColumnDefine * getColumnDefineNoExcept(const ColumnDefines & columns, ColumnID id) noexcept
{
    for (const auto & cd : columns)
    {
        if (cd.id == id)
        {
            return &cd;
        }
    }
    return nullptr;
}

const ColumnDefine & getColumnDefine(const ColumnDefines & columns, ColumnID id)
{
    const auto * cd = getColumnDefineNoExcept(columns, id);
    RUNTIME_CHECK_MSG(cd != nullptr, "ColumnID({}) not found", id);
    return *cd;
}

ColumnDefinesPtr getFilterColumns(
    const ColumnInfos & table_scan_column_infos,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    const ColumnDefines & table_scan_columns_to_read)
{
    if (filters.empty())
    {
        return nullptr;
    }
    // Get the columns of the filter, is a subset of table_scan_columns_to_read
    std::unordered_set<ColumnID> filter_col_ids;
    for (const auto & expr : filters)
    {
        getColumnIDsFromExpr(expr, table_scan_column_infos, filter_col_ids);
    }
    auto filter_columns = std::make_shared<DM::ColumnDefines>();
    filter_columns->reserve(filter_col_ids.size());
    for (const auto & id : filter_col_ids)
    {
        const auto & cd = getColumnDefine(table_scan_columns_to_read, id);
        filter_columns->emplace_back(cd);
    }
    return filter_columns;
}

DAGExpressionAnalyzer buildAnalyzer(
    const ColumnInfos & table_scan_column_infos,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context)
{
    // The source_columns_of_analyzer should be the same as the size of table_scan_column_infos
    // The table_scan_columns_to_read is a subset of table_scan_column_infos, when there are generated columns and extra table id column.
    NamesAndTypes source_columns_of_analyzer;
    source_columns_of_analyzer.reserve(table_scan_column_infos.size());
    for (size_t i = 0; i < table_scan_column_infos.size(); ++i)
    {
        auto const & ci = table_scan_column_infos[i];
        if (ci.hasGeneratedColumnFlag())
        {
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(ci);
            source_columns_of_analyzer.emplace_back(col_name, data_type);
        }
        else if (ci.id == EXTRA_TABLE_ID_COLUMN_ID)
        {
            source_columns_of_analyzer.emplace_back(EXTRA_TABLE_ID_COLUMN_NAME, EXTRA_TABLE_ID_COLUMN_TYPE);
        }
        else
        {
            const auto & cd = getColumnDefine(table_scan_columns_to_read, ci.id);
            source_columns_of_analyzer.emplace_back(cd.name, cd.type);
        }
    }
    return DAGExpressionAnalyzer{source_columns_of_analyzer, context};
}

std::vector<UInt8> buildMayNeedCastColumnBitmap(
    const ColumnInfos & table_scan_column_infos,
    const ColumnDefines & may_need_cast_columns)
{
    // may_need_add_cast_column should be the same size as table_scan_column_infos and source_columns_of_analyzer
    std::vector<UInt8> may_need_cast_column;
    may_need_cast_column.reserve(table_scan_column_infos.size());
    for (const auto & col : table_scan_column_infos)
    {
        may_need_cast_column.push_back(
            !col.hasGeneratedColumnFlag() && col.id != -1 && getColumnDefineNoExcept(may_need_cast_columns, col.id));
    }
    return may_need_cast_column;
}

NamesWithAliases buildProjectColumns(
    const ColumnInfos & table_scan_column_infos,
    const Strings & casted_columns,
    const ColumnDefines & need_project_cols)
{
    NamesWithAliases project_cols;
    for (size_t i = 0; i < table_scan_column_infos.size(); ++i)
    {
        if (const auto * cd = getColumnDefineNoExcept(need_project_cols, table_scan_column_infos[i].id); cd)
        {
            project_cols.emplace_back(casted_columns[i], cd->name);
        }
    }
    return project_cols;
}

ExpressionActionsPtr buildExtraCast(
    const ColumnInfos & table_scan_column_infos,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<UInt8> & may_need_add_cast_column,
    const ColumnDefines & need_project_cols)
{
    ExpressionActionsChain chain;
    auto & step = analyzer.initAndGetLastStep(chain);
    auto & actions = step.actions;
    if (auto [has_cast, casted_columns]
        = analyzer.buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan_column_infos);
        has_cast)
    {
        auto project_cols = buildProjectColumns(table_scan_column_infos, casted_columns, need_project_cols);
        actions->add(ExpressionAction::project(project_cols));

        for (const auto & col : need_project_cols)
            step.required_output.push_back(col.name);

        auto extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();
        return extra_cast;
    }
    return nullptr;
}

std::unordered_map<ColumnID, DataTypePtr> getCastedColumnTypes(
    const ColumnInfos & table_scan_column_infos,
    const ColumnDefines & table_scan_columns_to_read,
    DAGExpressionAnalyzer & analyzer)
{
    std::unordered_map<ColumnID, DataTypePtr> casted_column_types;
    const auto & current_names_and_types = analyzer.getCurrentInputColumns();
    for (size_t i = 0; i < table_scan_column_infos.size(); ++i)
    {
        if (table_scan_column_infos[i].hasGeneratedColumnFlag()
            || table_scan_column_infos[i].id == EXTRA_TABLE_ID_COLUMN_ID)
            continue;
        const auto & cd = getColumnDefine(table_scan_columns_to_read, table_scan_column_infos[i].id);
        RUNTIME_CHECK_MSG(
            cd.name == current_names_and_types[i].name,
            "Column name mismatch, expect: {}, actual: {}",
            cd.name,
            current_names_and_types[i].name);
        if (!cd.type->equals(*current_names_and_types[i].type))
        {
            casted_column_types.emplace(cd.id, current_names_and_types[i].type);
        }
    }
    return casted_column_types;
}

ColumnDefinesPtr removeColumnDefines(const ColumnDefines & columns, const ColumnDefines & to_remove)
{
    auto rest_columns = std::make_shared<ColumnDefines>();
    rest_columns->reserve(columns.size() - to_remove.size());
    for (const auto & cd : columns)
    {
        if (!getColumnDefineNoExcept(to_remove, cd.id))
        {
            rest_columns->push_back(cd);
        }
    }
    return rest_columns;
}

ColumnDefinesPtr buildCastedColumns(
    const ColumnDefines & columns,
    const std::unordered_map<ColumnID, DataTypePtr> & lm_casted_columns,
    const std::unordered_map<ColumnID, DataTypePtr> & rest_casted_columns)
{
    auto get_casted_type
        = [](ColumnID col_id, const std::unordered_map<ColumnID, DataTypePtr> & casted_columns) -> DataTypePtr {
        auto it = casted_columns.find(col_id);
        return it != casted_columns.end() ? it->second : nullptr;
    };
    auto casted_columns = std::make_shared<ColumnDefines>(columns);
    for (auto & cd : *casted_columns)
    {
        if (auto casted_type = get_casted_type(cd.id, lm_casted_columns); casted_type)
        {
            cd.type = casted_type;
        }
        else if (auto casted_type = get_casted_type(cd.id, rest_casted_columns); casted_type)
        {
            cd.type = casted_type;
        }
    }
    return casted_columns;
}

bool containsAllFilterColumns(const ColumnDefines & columns, const ColumnDefines & filter_columns)
{
    for (const auto & filter_col : filter_columns)
        if (!getColumnDefineNoExcept(columns, filter_col.id))
            return false;
    return true;
}
} // namespace

bool PredicateFilter::transformBlock(
    ExpressionActionsPtr & extra_cast,
    FilterTransformAction & filter_trans,
    [[maybe_unused]] ExpressionActions & project,
    const String & filter_column_name,
    Block & block,
    IColumn::Filter & filter_result,
    bool return_filter,
    bool all_match)
{
    if (extra_cast)
        extra_cast->execute(block);

    if (all_match)
    {
        filter_result.resize(0); //All
        return true;
    }

    FilterPtr f = nullptr;
    if (filter_trans.transform(block, f, return_filter))
    {
        // `f` points to a filter column in block.
        // This column will be destroy after projecting, so copy/swap it.
        if (return_filter)
        {
            if (f)
                filter_result.assign(*f); // Some, TODO: cannot swap, how to reduce copy?
            else
                filter_result.resize(0); // All
        }
        block.erase(filter_column_name);
        // project.execute(block);
        return true; // Some or All, according to the content of filter_result
    }
    return false; // None
}

std::pair<PredicateFilterPtr, std::unordered_map<ColumnID, DataTypePtr>> PredicateFilter::build(
    const ColumnDefines & filter_columns,
    const ColumnDefines & input_columns,
    const ColumnInfos & table_scan_column_infos,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context,
    const LoggerPtr & log)
{
    if (filters.empty())
    {
        LOG_DEBUG(log, "Push down filter is empty");
        return {};
    }

    // Build analyzer with all the columns of table scanning
    auto analyzer = buildAnalyzer(table_scan_column_infos, table_scan_columns_to_read, context);
    // Just columns to read with this filter need to cast
    auto may_need_add_cast_column = buildMayNeedCastColumnBitmap(table_scan_column_infos, filter_columns);

    // Currently, we merge the columns of lm and rest before executing the rest filters.
    // TODO: maybe executing the rest filters before merge.
    //const auto & need_project_cols
    //= filter_type == PredicateFilterType::LM ? filter_columns_to_read : table_scan_columns_to_read;
    auto extra_cast = buildExtraCast(table_scan_column_infos, analyzer, may_need_add_cast_column, input_columns);

    // build filter expression actions
    auto [before_where, filter_column_name, project_after_where] = analyzer.buildPushDownFilter(filters);

    const auto & actions = project_after_where->getActions();
    RUNTIME_CHECK(actions.size() == 1);
    RUNTIME_CHECK(actions.front().type == ExpressionAction::PROJECT);
    Names project_output_names;
    for (const auto & ci : table_scan_column_infos)
    {
        if (ci.hasGeneratedColumnFlag() || ci.id == EXTRA_TABLE_ID_COLUMN_ID)
        {
            continue;
        }
        const auto & cd = getColumnDefine(table_scan_columns_to_read, ci.id);
        project_output_names.push_back(cd.name);
    }
    const auto & project_input_columns = project_after_where->getRequiredColumnsWithTypes();
    project_after_where = std::make_shared<ExpressionActions>(project_input_columns);
    project_after_where->add(ExpressionAction::project(project_output_names));

    auto casted_column_types = extra_cast
        ? getCastedColumnTypes(table_scan_column_infos, table_scan_columns_to_read, analyzer)
        : std::unordered_map<ColumnID, DataTypePtr>{};

    LOG_DEBUG(
        log,
        "filter_columns={}, input_columns={}, extra_cast={}, before_where={}, table_scan_columns_to_read={} => {}, "
        "project={}",
        filter_columns,
        input_columns,
        extra_cast ? extra_cast->dumpActions() : "null",
        before_where->dumpActions(),
        table_scan_columns_to_read,
        casted_column_types,
        project_after_where->dumpActions());

    return {
        std::make_shared<PredicateFilter>(before_where, project_after_where, filter_column_name, extra_cast, log),
        std::move(casted_column_types)};
}

BlockInputStreamPtr PredicateFilter::buildFilterInputStream(BlockInputStreamPtr stream, bool need_project) const
{
    LOG_DEBUG(log, "buildFilterInputStream start: {}", stream->getHeader().dumpNames());
    if (extra_cast)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
        stream->setExtraInfo("extra cast after tablescanning");
        LOG_DEBUG(log, "buildFilterInputStream extra_cast: {}", stream->getHeader().dumpNames());
    }

    stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log->identifier());
    stream->setExtraInfo("push down filter");
    LOG_DEBUG(log, "buildFilterInputStream filter: {}", stream->getHeader().dumpNames());

    if (need_project)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log->identifier());
        stream->setExtraInfo("project after where");
        LOG_DEBUG(log, "buildFilterInputStream project: {}", stream->getHeader().dumpNames());
    }
    return stream;
}

PushDownFilterPtr PushDownFilter::build(
    const RSOperatorPtr & rs_operator,
    const ColumnInfos & table_scan_column_infos,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & lm_filter_exprs,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & rest_filter_exprs,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context,
    const bool keep_order,
    const LoggerPtr & tracing_logger)
{
    // Must use the original `table_scan_column_infos` because exprs will get column info by index.
    auto lm_columns = getFilterColumns(table_scan_column_infos, lm_filter_exprs, table_scan_columns_to_read);
    PredicateFilterPtr lm_filter;
    std::unordered_map<ColumnID, DataTypePtr> lm_casted_columns;
    if (lm_columns)
    {
        std::tie(lm_filter, lm_casted_columns) = PredicateFilter::build(
            /*filter_columns*/ *lm_columns,
            /*input_columns*/ *lm_columns,
            table_scan_column_infos,
            lm_filter_exprs,
            table_scan_columns_to_read,
            context,
            tracing_logger->getChild("LM"));
    }

    auto rest_columns = lm_columns ? removeColumnDefines(table_scan_columns_to_read, *lm_columns)
                                   : std::make_shared<ColumnDefines>(table_scan_columns_to_read);
    PredicateFilterPtr rest_filter;
    std::unordered_map<ColumnID, DataTypePtr> rest_casted_columns;
    bool push_down_rest_filter = false;
    if (::DB::pushDownAllFilters(context.getSettingsRef().force_push_down_all_filters_to_scan, keep_order)
        && !rest_filter_exprs.empty())
    {
        auto rest_filter_columns
            = getFilterColumns(table_scan_column_infos, rest_filter_exprs, table_scan_columns_to_read);
        push_down_rest_filter = containsAllFilterColumns(*rest_columns, *rest_filter_columns);
        const auto & input_columns = push_down_rest_filter ? *rest_columns : table_scan_columns_to_read;

        std::tie(rest_filter, rest_casted_columns) = PredicateFilter::build(
            *rest_filter_columns,
            input_columns,
            table_scan_column_infos,
            rest_filter_exprs,
            table_scan_columns_to_read,
            context,
            tracing_logger->getChild("Rest"));
    }
    auto casted_columns = buildCastedColumns(table_scan_columns_to_read, lm_casted_columns, rest_casted_columns);
    LOG_DEBUG(
        tracing_logger,
        "columns_to_read: {} => {}, push_down_rest_filter: {}",
        table_scan_columns_to_read,
        *casted_columns,
        push_down_rest_filter);
    return std::make_shared<PushDownFilter>(
        rs_operator,
        lm_filter,
        rest_filter,
        push_down_rest_filter,
        lm_columns,
        rest_columns,
        casted_columns);
}

PushDownFilterPtr PushDownFilter::build(
    const SelectQueryInfo & query_info,
    const ColumnDefines & table_scan_columns_to_read,
    const ColumnDefines & table_column_defines,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    const auto & dag_query = query_info.dag_query;
    if (unlikely(dag_query == nullptr))
        return EMPTY_FILTER;

    const auto & table_scan_column_infos = dag_query->source_columns;
    const auto rs_operator = RSOperator::build(
        dag_query,
        table_scan_column_infos,
        table_column_defines,
        context.getSettingsRef().dt_enable_rough_set_filter,
        tracing_logger);

    return PushDownFilter::build(
        rs_operator,
        table_scan_column_infos,
        dag_query->pushed_down_filters,
        dag_query->filters,
        table_scan_columns_to_read,
        context,
        query_info.keep_order,
        tracing_logger);
}
} // namespace DB::DM

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
#include <memory>

namespace DB::DM
{
namespace
{
const ColumnDefine * getColumnDefineNoExcept(const ColumnDefines & cds, ColumnID id) noexcept
{
    for (const auto & cd : cds)
    {
        if (cd.id == id)
        {
            return &cd;
        }
    }
    return nullptr;
}

const ColumnDefine & getColumnDefine(const ColumnDefines & cds, ColumnID id)
{
    const auto * cd = getColumnDefineNoExcept(cds, id);
    RUNTIME_CHECK_MSG(cd != nullptr, "ColumnID({}) not found", id);
    return *cd;
}

ColumnDefinesPtr getFilterColumns(
    const ColumnInfos & table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    const ColumnDefines & columns_to_read)
{
    // Get the columns of the filter, is a subset of columns_to_read
    std::unordered_set<ColumnID> filter_col_ids;
    for (const auto & expr : filters)
    {
        getColumnIDsFromExpr(expr, table_scan_column_info, filter_col_ids);
    }
    auto filter_columns = std::make_shared<DM::ColumnDefines>();
    filter_columns->reserve(filter_col_ids.size());
    for (const auto & id : filter_col_ids)
    {
        const auto & cd = getColumnDefine(columns_to_read, id);
        filter_columns->emplace_back(cd);
    }
    return filter_columns;
}

NamesAndTypes buildSourceColumnsOfAnalyzer(
    const ColumnInfos & table_scan_column_info,
    const ColumnDefines & columns_to_read)
{
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
        if (cid == EXTRA_TABLE_ID_COLUMN_ID)
        {
            source_columns_of_analyzer.emplace_back(EXTRA_TABLE_ID_COLUMN_NAME, EXTRA_TABLE_ID_COLUMN_TYPE);
            continue;
        }
        const auto & cd = getColumnDefine(columns_to_read, cid);
        source_columns_of_analyzer.emplace_back(cd.name, cd.type);
    }
    return source_columns_of_analyzer;
}

std::vector<UInt8> buildMayNeedCastColumnBitmap(
    const ColumnInfos & table_scan_column_info,
    const ColumnDefines & may_need_cast_columns)
{
    // may_need_add_cast_column should be the same size as table_scan_column_info and source_columns_of_analyzer
    std::vector<UInt8> may_need_cast_column_bitmap;
    may_need_cast_column_bitmap.reserve(table_scan_column_info.size());
    for (const auto & col : table_scan_column_info)
        may_need_cast_column_bitmap.push_back(
            !col.hasGeneratedColumnFlag() && getColumnDefineNoExcept(may_need_cast_columns, col.id) && col.id != -1);
    return may_need_cast_column_bitmap;
}

std::vector<UInt8> buildMayNeedCastColumnBitmap(
    const ColumnInfos & table_scan_column_info,
    const std::unordered_set<ColumnID> & may_need_cast_columns)
{
    // may_need_add_cast_column should be the same size as table_scan_column_info and source_columns_of_analyzer
    std::vector<UInt8> may_need_cast_column_bitmap;
    may_need_cast_column_bitmap.reserve(table_scan_column_info.size());
    for (const auto & col : table_scan_column_info)
        may_need_cast_column_bitmap.push_back(
            !col.hasGeneratedColumnFlag() && may_need_cast_columns.contains(col.id) && col.id != -1);
    return may_need_cast_column_bitmap;
}

NamesWithAliases buildProjectColumns(
    const ColumnDefines & columns_to_read,
    const Strings & casted_columns,
    const ColumnDefines & need_project_cols)
{
    NamesWithAliases project_cols;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (getColumnDefineNoExcept(need_project_cols, columns_to_read[i].id))
        {
            project_cols.emplace_back(casted_columns[i], columns_to_read[i].name);
        }
    }
    return project_cols;
}

ExpressionActionsPtr buildExtraCast(
    const ColumnInfos & table_scan_column_info,
    const ColumnDefines & columns_to_read,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<UInt8> & may_need_add_cast_column,
    const ColumnDefines & source_columns)
{
    ExpressionActionsChain chain;
    auto & step = analyzer.initAndGetLastStep(chain);
    auto & actions = step.actions;
    if (auto [has_cast, casted_columns]
        = analyzer.buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan_column_info);
        has_cast)
    {
        auto project_cols = buildProjectColumns(columns_to_read, casted_columns, source_columns);
        actions->add(ExpressionAction::project(project_cols));

        for (const auto & col : source_columns)
            step.required_output.push_back(col.name);

        auto extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();
        return extra_cast;
    }
    return nullptr;
}
} // namespace

QueryFilterPtr QueryFilter::build(
    QueryFilterType filter_type,
    const ColumnInfos & table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    const ColumnDefines & columns_to_read,
    const Context & context,
    const LoggerPtr & tracing_logger,
    const std::unordered_set<ColumnID> & rest_columns_to_read)
{
    RUNTIME_CHECK(
        filter_type == QueryFilterType::LM && rest_columns_to_read.empty()
        || filter_type == QueryFilterType::Rest && !rest_columns_to_read.empty());
    auto log = tracing_logger->getChild(magic_enum::enum_name(filter_type));
    if (filters.empty())
    {
        LOG_DEBUG(log, "Push down filter is empty");
        return nullptr;
    }

    auto filter_columns = getFilterColumns(table_scan_column_info, filters, columns_to_read);

    auto source_columns_of_analyzer = buildSourceColumnsOfAnalyzer(table_scan_column_info, columns_to_read);
    auto analyzer = std::make_unique<DAGExpressionAnalyzer>(source_columns_of_analyzer, context);
    auto may_need_add_cast_column = filter_type == QueryFilterType::LM
        ? buildMayNeedCastColumnBitmap(table_scan_column_info, *filter_columns)
        : buildMayNeedCastColumnBitmap(table_scan_column_info, rest_columns_to_read);
    auto extra_cast = buildExtraCast(
        table_scan_column_info,
        columns_to_read,
        *analyzer,
        may_need_add_cast_column,
        filter_type == QueryFilterType::LM ? *filter_columns : columns_to_read);

    // build filter expression actions
    auto [before_where, filter_column_name, project_after_where] = analyzer->buildPushDownFilter(filters);

    // record current column defines
    auto columns_after_cast = std::make_shared<ColumnDefines>();
    if (extra_cast != nullptr)
    {
        columns_after_cast->reserve(columns_to_read.size());
        const auto & current_names_and_types = analyzer->getCurrentInputColumns();
        for (size_t i = 0; i < table_scan_column_info.size(); ++i)
        {
            if (table_scan_column_info[i].hasGeneratedColumnFlag()
                || table_scan_column_info[i].id == EXTRA_TABLE_ID_COLUMN_ID)
                continue;
            const auto & cd = getColumnDefine(columns_to_read, table_scan_column_info[i].id);
            RUNTIME_CHECK_MSG(
                cd.name == current_names_and_types[i].name,
                "Column name mismatch, expect: {}, actual: {}",
                cd.name,
                current_names_and_types[i].name);
            columns_after_cast->push_back(cd);
            columns_after_cast->back().type = current_names_and_types[i].type;
        }
    }
    LOG_DEBUG(
        log,
        "extra_cast={}, before_where={}, columns_to_read={} => {}, project={}, ",
        extra_cast ? extra_cast->dumpActions() : "null",
        before_where->dumpActions(),
        columns_to_read,
        *columns_after_cast,
        project_after_where->dumpActions());

    return std::make_shared<QueryFilter>(
        filter_type,
        before_where,
        project_after_where,
        filter_columns,
        filter_column_name,
        extra_cast,
        columns_after_cast);
}

BlockInputStreamPtr QueryFilter::buildFilterInputStream(
    BlockInputStreamPtr stream,
    bool need_project,
    const String & tracing_id) const
{
    auto filter_name = magic_enum::enum_name(filter_type);
    auto log = Logger::get(fmt::format("{} {}", tracing_id, filter_name));
    LOG_DEBUG(log, "buildFilterInputStream: {}", stream->getHeader().dumpNames());
    if (extra_cast)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, tracing_id);
        stream->setExtraInfo(fmt::format("{}: cast after tablescanning", filter_name));
        LOG_DEBUG(log, "buildFilterInputStream: {}", stream->getHeader().dumpNames());
    }

    stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, tracing_id);
    stream->setExtraInfo(fmt::format("{}: push down filter", filter_name));
    LOG_DEBUG(log, "buildFilterInputStream: {}", stream->getHeader().dumpNames());

    if (need_project)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, tracing_id);
        stream->setExtraInfo(fmt::format("{}: project after where", filter_name));
        LOG_DEBUG(log, "buildFilterInputStream: {}", stream->getHeader().dumpNames());
    }
    return stream;
}

PushDownFilterPtr PushDownFilter::build(
    const RSOperatorPtr & rs_operator,
    const ColumnInfos & table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
    const ColumnDefines & columns_to_read,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    auto lm_filter = QueryFilter::build(
        QueryFilterType::LM,
        table_scan_column_info,
        pushed_down_filters,
        columns_to_read,
        context,
        tracing_logger,
        {});
    return std::make_shared<PushDownFilter>(rs_operator, lm_filter, nullptr);
}

PushDownFilterPtr PushDownFilter::build(
    const SelectQueryInfo & query_info,
    const ColumnDefines & columns_to_read,
    const ColumnDefines & table_column_defines,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    const auto & dag_query = query_info.dag_query;
    if (unlikely(dag_query == nullptr))
        return EMPTY_FILTER;

    const auto rs_operator = RSOperator::build(
        dag_query,
        columns_to_read,
        table_column_defines,
        context.getSettingsRef().dt_enable_rough_set_filter,
        tracing_logger);

    const auto & columns_to_read_info = dag_query->source_columns;
    auto lm_filter = QueryFilter::build(
        QueryFilterType::LM,
        columns_to_read_info,
        dag_query->pushed_down_filters,
        columns_to_read,
        context,
        tracing_logger,
        {});

    // TODO: fix columns_after_cast when force_push_down_all_filters_to_scan is true.
    std::unordered_set<ColumnID> rest_columns_to_read;
    for (const auto & cd : columns_to_read)
    {
        if (!lm_filter)
        {
            rest_columns_to_read.insert(cd.id);
            continue;
        }

        auto it = std::find_if(
            lm_filter->filter_columns->cbegin(),
            lm_filter->filter_columns->cend(),
            [&cd](const auto & filter_cd) { return cd.id == filter_cd.id; });
        if (it == lm_filter->filter_columns->cend())
        {
            rest_columns_to_read.insert(cd.id);
        }
    }
    auto rest_filter = context.getSettingsRef().force_push_down_all_filters_to_scan //
        ? QueryFilter::build(
            QueryFilterType::Rest,
            columns_to_read_info,
            dag_query->filters,
            columns_to_read,
            context,
            tracing_logger,
            rest_columns_to_read)
        : nullptr;

    return std::make_shared<PushDownFilter>(rs_operator, lm_filter, rest_filter);
}
} // namespace DB::DM

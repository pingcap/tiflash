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
        getColumnIDsFromExpr(expr, table_scan_column_info, filter_col_ids);
    }
    auto filter_columns = std::make_shared<DM::ColumnDefines>();
    filter_columns->reserve(filter_col_ids.size());
    for (const auto & id : filter_col_ids)
    {
        const auto & cd = getColumnDefine(table_scan_columns_to_read, id); // TODO: what if id is a virtual columns
        filter_columns->emplace_back(cd);
    }
    return filter_columns;
}

std::unique_ptr<DAGExpressionAnalyzer> buildAnalyzer(
    const ColumnInfos & table_scan_column_info,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context)
{
    // The source_columns_of_analyzer should be the same as the size of table_scan_column_info
    // The table_scan_columns_to_read is a subset of table_scan_column_info, when there are generated columns and extra table id column.
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
        const auto & cd = getColumnDefine(table_scan_columns_to_read, cid);
        source_columns_of_analyzer.emplace_back(cd.name, cd.type);
    }
    return std::make_unique<DAGExpressionAnalyzer>(source_columns_of_analyzer, context);
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

NamesWithAliases buildProjectColumns(
    const ColumnInfos & table_scan_column_info,
    const Strings & casted_columns,
    const ColumnDefines & need_project_cols)
{
    NamesWithAliases project_cols;
    for (size_t i = 0; i < table_scan_column_info.size(); ++i)
    {
        if (const auto * cd = getColumnDefineNoExcept(need_project_cols, table_scan_column_info[i].id); cd)
        {
            project_cols.emplace_back(casted_columns[i], cd->name);
        }
    }
    return project_cols;
}

ExpressionActionsPtr buildExtraCast(
    const ColumnInfos & table_scan_column_info,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<UInt8> & may_need_add_cast_column,
    const ColumnDefines & need_project_cols)
{
    ExpressionActionsChain chain;
    auto & step = analyzer.initAndGetLastStep(chain);
    auto & actions = step.actions;
    if (auto [has_cast, casted_columns]
        = analyzer.buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan_column_info);
        has_cast)
    {
        auto project_cols = buildProjectColumns(table_scan_column_info, casted_columns, need_project_cols);
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
    const ColumnInfos & table_scan_column_info,
    const ColumnDefines & table_scan_columns_to_read,
    DAGExpressionAnalyzer & analyzer)
{
    std::unordered_map<ColumnID, DataTypePtr> casted_column_types;
    const auto & current_names_and_types = analyzer.getCurrentInputColumns();
    for (size_t i = 0; i < table_scan_column_info.size(); ++i)
    {
        if (table_scan_column_info[i].hasGeneratedColumnFlag()
            || table_scan_column_info[i].id == EXTRA_TABLE_ID_COLUMN_ID)
            continue;
        const auto & cd = getColumnDefine(table_scan_columns_to_read, table_scan_column_info[i].id);
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
    const QueryFilterPtr & lm_filter,
    const QueryFilterPtr & rest_filter)
{
    auto get_casted_type = [](ColumnID col_id, const QueryFilterPtr & filter) -> DataTypePtr {
        if (!filter || filter->casted_column_types.empty())
        {
            return nullptr;
        }
        auto it = filter->casted_column_types.find(col_id);
        return it != filter->casted_column_types.end() ? it->second : nullptr;
    };
    auto casted_columns = std::make_shared<ColumnDefines>(columns);
    for (auto & cd : *casted_columns)
    {
        if (auto casted_type = get_casted_type(cd.id, lm_filter); casted_type)
        {
            cd.type = casted_type;
        }
        else if (auto casted_type = get_casted_type(cd.id, rest_filter); casted_type)
        {
            cd.type = casted_type;
        }
    }

    if (rest_filter)
    {
        for (const auto & [id, name, type] : rest_filter->generated_column_infos)
        {
            casted_columns->emplace_back(id, name, type);
        }
    }
    return casted_columns;
}

std::vector<std::tuple<UInt64, String, DataTypePtr>> getGeneratedColumnInfos(const ColumnInfos & column_infos)
{
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    for (size_t i = 0; i < column_infos.size(); ++i)
    {
        const auto & ci = column_infos[i];
        if (ci.hasGeneratedColumnFlag())
        {
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(ci);
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            generated_column_infos.emplace_back(i, col_name, data_type);
        }
    }
    return generated_column_infos;
}
} // namespace

QueryFilterPtr QueryFilter::build(
    QueryFilterType filter_type,
    const ColumnDefines & filter_columns_to_read,
    const ColumnInfos & table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    auto log = tracing_logger->getChild(magic_enum::enum_name(filter_type));
    if (filters.empty())
    {
        LOG_DEBUG(log, "Push down filter is empty");
        return nullptr;
    }

    auto generated_column_infos = filter_type == QueryFilterType::Rest
        ? getGeneratedColumnInfos(table_scan_column_info)
        : std::vector<std::tuple<UInt64, String, DataTypePtr>>{};

    // Build analyzer with all the columns of table scanning
    auto analyzer = buildAnalyzer(table_scan_column_info, table_scan_columns_to_read, context);
    // Just columns to read with this filter need to cast
    auto may_need_add_cast_column = buildMayNeedCastColumnBitmap(table_scan_column_info, filter_columns_to_read);

    // Currently, we merge the columns of lm and rest before executing the rest filters.
    // TODO: maybe executing the rest filters before merge.
    const auto & need_project_cols
        = filter_type == QueryFilterType::LM ? filter_columns_to_read : table_scan_columns_to_read;
    auto extra_cast = buildExtraCast(table_scan_column_info, *analyzer, may_need_add_cast_column, need_project_cols);

    // build filter expression actions
    auto [before_where, filter_column_name, project_after_where] = analyzer->buildPushDownFilter(filters);

    auto casted_column_types = extra_cast
        ? getCastedColumnTypes(table_scan_column_info, table_scan_columns_to_read, *analyzer)
        : std::unordered_map<ColumnID, DataTypePtr>{};

    LOG_DEBUG(
        log,
        "filter_columns_to_read={}, extra_cast={}, before_where={}, table_scan_columns_to_read={} => {}, "
        "project={} generated_column_infos={}",
        filter_columns_to_read,
        extra_cast ? extra_cast->dumpActions() : "null",
        before_where->dumpActions(),
        table_scan_columns_to_read,
        casted_column_types,
        project_after_where->dumpActions(),
        generated_column_infos);

    return std::make_shared<QueryFilter>(
        filter_type,
        before_where,
        project_after_where,
        filter_column_name,
        extra_cast,
        std::move(casted_column_types),
        std::move(generated_column_infos));
}

BlockInputStreamPtr QueryFilter::buildFilterInputStream(
    BlockInputStreamPtr stream,
    bool need_project,
    const String & tracing_id) const
{
    auto filter_name = magic_enum::enum_name(filter_type);
    auto log = Logger::get(fmt::format("{} {}", tracing_id, filter_name));
    LOG_DEBUG(log, "buildFilterInputStream start: {}", stream->getHeader().dumpNames());

    if (!generated_column_infos.empty())
    {
        stream
            = std::make_shared<GeneratedColumnPlaceholderBlockInputStream>(stream, generated_column_infos, tracing_id);
        stream->setExtraInfo(fmt::format("{}: generated column placeholder above table scan", filter_name));
        LOG_DEBUG(log, "buildFilterInputStream generated_column_infos: {}", stream->getHeader().dumpNames());
    }

    if (extra_cast)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, tracing_id);
        stream->setExtraInfo(fmt::format("{}: cast after tablescanning", filter_name));
        LOG_DEBUG(log, "buildFilterInputStream extra_cast: {}", stream->getHeader().dumpNames());
    }

    stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, tracing_id);
    stream->setExtraInfo(fmt::format("{}: push down filter", filter_name));
    LOG_DEBUG(log, "buildFilterInputStream filter: {}", stream->getHeader().dumpNames());

    if (need_project)
    {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, tracing_id);
        stream->setExtraInfo(fmt::format("{}: project after where", filter_name));
        LOG_DEBUG(log, "buildFilterInputStream project: {}", stream->getHeader().dumpNames());
    }
    return stream;
}

PushDownFilterPtr PushDownFilter::build(
    const RSOperatorPtr & rs_operator,
    const ColumnInfos & _table_scan_column_info,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & lm_filter_exprs,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & rest_filter_exprs,
    const ColumnDefines & table_scan_columns_to_read,
    const Context & context,
    const LoggerPtr & tracing_logger)
{
    ColumnInfos new_column_infos;
    for (const auto & ci : _table_scan_column_info)
    {
        if (!ci.hasGeneratedColumnFlag())
        {
            new_column_infos.push_back(ci);
        }
    }
    auto lm_columns = getFilterColumns(new_column_infos, lm_filter_exprs, table_scan_columns_to_read);

    auto lm_filter = lm_columns //
        ? QueryFilter::build(
            QueryFilterType::LM,
            *lm_columns,
            new_column_infos,
            lm_filter_exprs,
            table_scan_columns_to_read,
            context,
            tracing_logger)
        : nullptr;

    auto rest_columns = lm_columns ? removeColumnDefines(table_scan_columns_to_read, *lm_columns)
                                   : std::make_shared<ColumnDefines>(table_scan_columns_to_read);
    auto rest_filter = context.getSettingsRef().force_push_down_all_filters_to_scan && !rest_filter_exprs.empty()
        ? QueryFilter::build(
            QueryFilterType::Rest,
            *rest_columns,
            new_column_infos,
            rest_filter_exprs,
            table_scan_columns_to_read,
            context,
            tracing_logger)
        : nullptr;
    auto casted_columns = buildCastedColumns(table_scan_columns_to_read, lm_filter, rest_filter);
    LOG_DEBUG(tracing_logger, "casted_columns={}", *casted_columns);
    return std::make_shared<PushDownFilter>(
        rs_operator,
        lm_filter,
        rest_filter,
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

    const auto & table_scan_column_info = dag_query->source_columns;
    const auto rs_operator = RSOperator::build(
        dag_query,
        table_scan_column_info,
        table_column_defines,
        context.getSettingsRef().dt_enable_rough_set_filter,
        tracing_logger);

    return PushDownFilter::build(
        rs_operator,
        table_scan_column_info,
        dag_query->pushed_down_filters,
        dag_query->filters,
        table_scan_columns_to_read,
        context,
        tracing_logger);
}
} // namespace DB::DM

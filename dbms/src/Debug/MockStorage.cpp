// Copyright 2023 PingCAP, Inc.
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
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/Context.h>
#include <Operators/FilterTransformOp.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationRuntimeStats.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/MutableSupport.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>
#include <TiDB/Decode/TypeMapping.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{
namespace
{
String getColumnNameForDeltaMergeRead(const MockColumnInfoVec & table_schema, const TiDB::ColumnInfo & ci)
{
    const ColumnID cid = ci.id;
    if (cid == TiDBPkColumnID)
        return MutableSupport::tidb_pk_column_name;
    if (cid == ExtraTableIDColumnID)
        return MutableSupport::extra_table_id_column_name;
    if (cid == ExtraCommitTSColumnID)
        return MutableSupport::version_column_name;
    if (!ci.name.empty())
        return ci.name;
    RUNTIME_CHECK_MSG(cid >= 0 && static_cast<size_t>(cid) < table_schema.size(), "Invalid mock column id {}", cid);
    return table_schema[cid].name;
}

Names getColumnNamesForDeltaMergeRead(
    const MockColumnInfoVec & table_schema,
    const TiDB::ColumnInfos & scan_column_infos)
{
    Names column_names;
    column_names.reserve(scan_column_infos.size());
    for (const auto & ci : scan_column_infos)
        column_names.push_back(getColumnNameForDeltaMergeRead(table_schema, ci));
    return column_names;
}

NamesAndTypes getNameAndTypesForDeltaMergeRead(
    const MockColumnInfoVec & table_schema,
    const TiDB::ColumnInfos & scan_column_infos)
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(scan_column_infos.size());
    for (const auto & ci : scan_column_infos)
        names_and_types.emplace_back(
            getColumnNameForDeltaMergeRead(table_schema, ci),
            getDataTypeByColumnInfoForComputingLayer(ci));
    return names_and_types;
}

std::vector<UInt8> getMayNeedAddCastColumnAfterTableScan(
    const TiDB::ColumnInfos & scan_column_infos,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
    const FilterConditions * filter_conditions,
    const Context & context,
    bool enable_multi_stage_late_materialization)
{
    std::unordered_set<ColumnID> filter_col_id_set;
    if (!enable_multi_stage_late_materialization)
    {
        for (const auto & expr : pushed_down_filters)
            getColumnIDsFromExpr(expr, scan_column_infos, filter_col_id_set);
        if (unlikely(context.getSettingsRef().force_push_down_all_filters_to_scan) && filter_conditions != nullptr)
        {
            for (const auto & expr : filter_conditions->conditions)
                getColumnIDsFromExpr(expr, scan_column_infos, filter_col_id_set);
        }
    }

    std::vector<UInt8> may_need_add_cast_column;
    may_need_add_cast_column.reserve(scan_column_infos.size());
    for (const auto & col : scan_column_infos)
        may_need_add_cast_column.push_back(
            !col.hasGeneratedColumnFlag() && !filter_col_id_set.contains(col.id) && col.id != -1);
    return may_need_add_cast_column;
}

ExpressionActionsPtr buildExtraCastsAfterTableScan(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<UInt8> & may_need_add_cast_column,
    const TiDB::ColumnInfos & scan_column_infos)
{
    auto adjusted_may_need_add_cast_column = may_need_add_cast_column;
    const auto & source_columns = analyzer.getCurrentInputColumns();
    const auto columns_size = std::min(adjusted_may_need_add_cast_column.size(), source_columns.size());
    for (size_t i = 0; i < columns_size; ++i)
    {
        if (!adjusted_may_need_add_cast_column[i] || scan_column_infos[i].tp != TiDB::TypeTime)
            continue;
        if (checkAndGetDataType<DataTypeMyDuration>(removeNullable(source_columns[i].type).get()) != nullptr)
            adjusted_may_need_add_cast_column[i] = false;
    }

    if (std::find(adjusted_may_need_add_cast_column.begin(), adjusted_may_need_add_cast_column.end(), true)
        == adjusted_may_need_add_cast_column.end())
        return nullptr;

    ExpressionActionsChain chain;
    auto & step = analyzer.initAndGetLastStep(chain);
    auto & actions = step.actions;
    auto [has_cast, casted_columns]
        = analyzer.buildExtraCastsAfterTS(actions, adjusted_may_need_add_cast_column, scan_column_infos);
    if (!has_cast)
        return nullptr;

    NamesWithAliases project_cols;
    project_cols.reserve(casted_columns.size());
    for (size_t i = 0; i < casted_columns.size(); ++i)
        project_cols.emplace_back(casted_columns[i], source_columns[i].name);
    actions->add(ExpressionAction::project(project_cols));

    for (const auto & col : source_columns)
        step.required_output.push_back(col.name);

    auto extra_cast = chain.getLastActions();
    chain.finalize();
    chain.clear();
    return extra_cast;
}

bool shouldEnableMultiStageLateMaterializationForMockDeltaMerge(
    const Context & context,
    bool keep_order,
    const FilterConditions * filter_conditions,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
    const TiDB::ColumnInfos & scan_column_infos)
{
    const auto & settings = context.getSettingsRef();
    if (!settings.dt_enable_multi_stage_late_materialization)
        return false;
    if (filter_conditions == nullptr || !filter_conditions->hasValue())
        return false;
    if (filter_conditions->conditions.empty())
        return false;
    if (pushed_down_filters.empty())
        return false;
    if (settings.force_push_down_all_filters_to_scan)
        return false;
    if (!settings.dt_enable_bitmap_filter)
        return false;
    if (keep_order)
        return false;

    std::unordered_set<ColumnID> stage0_filter_col_id_set;
    for (const auto & expr : pushed_down_filters)
        getColumnIDsFromExpr(expr, scan_column_infos, stage0_filter_col_id_set);

    bool stage0_filter_covers_all_scan_columns = true;
    for (const auto & col : scan_column_infos)
    {
        if (col.id == ExtraTableIDColumnID)
            continue;
        if (!stage0_filter_col_id_set.contains(col.id))
        {
            stage0_filter_covers_all_scan_columns = false;
            break;
        }
    }
    if (stage0_filter_covers_all_scan_columns)
        return false;

    std::unordered_set<ColumnID> stage1_filter_col_id_set;
    for (const auto & expr : filter_conditions->conditions)
        getColumnIDsFromExpr(expr, scan_column_infos, stage1_filter_col_id_set);
    if (stage1_filter_col_id_set.contains(ExtraTableIDColumnID))
        return false;

    const auto stage1_filter_col_cnt = stage1_filter_col_id_set.size();
    if (stage1_filter_col_cnt == 0 || stage1_filter_col_cnt > 10)
        return false;

    size_t final_rest_col_cnt = 0;
    for (const auto & col : scan_column_infos)
    {
        if (col.id == ExtraTableIDColumnID)
            continue;
        if (!stage1_filter_col_id_set.contains(col.id))
            ++final_rest_col_cnt;
    }
    return final_rest_col_cnt >= 12 && final_rest_col_cnt >= 3 * stage1_filter_col_cnt;
}
} // namespace

/// for table scan
void MockStorage::addTableSchema(const String & name, const MockColumnInfoVec & columnInfos)
{
    name_to_id_map[name] = MockTableIdGenerator::instance().nextTableId();
    table_schema[getTableId(name)] = columnInfos;
    addTableInfo(name, columnInfos);
}

void MockStorage::addTableData(const String & name, ColumnsWithTypeAndName & columns)
{
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i].column_id = i;

    table_columns[getTableId(name)] = columns;
}

void MockStorage::addTableScanConcurrencyHint(const String & name, size_t concurrency_hint)
{
    table_scan_concurrency_hint[getTableId(name)] = concurrency_hint;
}

void MockStorage::addDeltaMergeTableConcurrencyHint(const String & name, size_t concurrency_hint)
{
    delta_merge_table_id_to_concurrency_hint[getTableIdForDeltaMerge(name)] = concurrency_hint;
}

Int64 MockStorage::getTableId(const String & name)
{
    if (name_to_id_map.find(name) != name_to_id_map.end())
    {
        return name_to_id_map[name];
    }
    throw Exception(fmt::format("Failed to get table id by table name '{}'", name));
}

bool MockStorage::tableExists(Int64 table_id)
{
    return table_schema.find(table_id) != table_schema.end();
}

ColumnsWithTypeAndName MockStorage::getColumns(Int64 table_id)
{
    if (tableExists(table_id))
    {
        return table_columns[table_id];
    }
    throw Exception(fmt::format("Failed to get columns by table_id '{}'", table_id));
}

size_t MockStorage::getScanConcurrencyHint(Int64 table_id)
{
    if (tableExists(table_id))
    {
        return table_scan_concurrency_hint[table_id];
    }
    return 0;
}

size_t MockStorage::getDelatMergeTableConcurrencyHint(Int64 table_id)
{
    if (tableExistsForDeltaMerge(table_id))
    {
        return delta_merge_table_id_to_concurrency_hint[table_id];
    }
    return 0;
}

MockColumnInfoVec MockStorage::getTableSchema(const String & name)
{
    if (tableExists(getTableId(name)))
    {
        return table_schema[getTableId(name)];
    }
    throw Exception(fmt::format("Failed to get table schema by table name '{}'", name));
}

/// for delta merge
Int64 MockStorage::addTableSchemaForDeltaMerge(const String & name, const MockColumnInfoVec & columnInfos)
{
    auto table_id = MockTableIdGenerator::instance().nextTableId();
    name_to_id_map_for_delta_merge[name] = table_id;
    table_schema_for_delta_merge[getTableIdForDeltaMerge(name)] = columnInfos;
    addTableInfoForDeltaMerge(name, columnInfos);
    return table_id;
}

Int64 MockStorage::addTableDataForDeltaMerge(Context & context, const String & name, ColumnsWithTypeAndName & columns)
{
    auto table_id = getTableIdForDeltaMerge(name);
    addNamesAndTypesForDeltaMerge(table_id, columns);
    if (storage_delta_merge_map.find(table_id) == storage_delta_merge_map.end())
    {
        // init
        ASTPtr astptr(new ASTIdentifier(name, ASTIdentifier::Kind::Table));
        NamesAndTypesList names_and_types_list;
        for (const auto & column : columns)
        {
            names_and_types_list.emplace_back(column.name, column.type);
        }
        astptr->children.emplace_back(new ASTIdentifier(columns[0].name));

        storage_delta_merge_map[table_id] = StorageDeltaMerge::create(
            "TiFlash",
            /* db_name= */ "default",
            name,
            std::nullopt,
            ColumnsDescription{names_and_types_list},
            astptr,
            0,
            context);

        auto storage = storage_delta_merge_map[table_id];
        assert(storage);
        storage->startup();

        // write data to DeltaMergeStorage
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, context.getSettingsRef());

        Block insert_block{columns};

        output->writePrefix();
        output->write(insert_block);
        output->writeSuffix();
    }
    return table_id;
}

std::tuple<StorageDeltaMergePtr, Names, SelectQueryInfo> MockStorage::prepareForRead(
    Context & context,
    Int64 table_id,
    bool keep_order)
{
    assert(tableExistsForDeltaMerge(table_id));
    auto storage = storage_delta_merge_map[table_id];
    auto & column_infos = table_schema_for_delta_merge[table_id];
    assert(storage);
    assert(!column_infos.empty());
    Names column_names;
    column_names.reserve(column_infos.size());
    for (const auto & column_info : column_infos)
        column_names.push_back(column_info.name);

    auto scan_context = std::make_shared<DM::ScanContext>();
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.keep_order = keep_order;
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(
        context.getSettingsRef().resolve_locks,
        std::numeric_limits<UInt64>::max(),
        scan_context);
    return {storage, column_names, query_info};
}

BlockInputStreamPtr MockStorage::getStreamFromDeltaMerge(
    Context & context,
    Int64 table_id,
    const FilterConditions * filter_conditions,
    bool keep_order,
    std::vector<int> runtime_filter_ids,
    int rf_max_wait_time_ms,
    const google::protobuf::RepeatedPtrField<tipb::Expr> * pushed_down_filters,
    const TiDB::ColumnInfos * table_scan_column_infos)
{
    static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_pushed_down_filters{};
    static const auto empty_ann_query_info = tipb::ANNQueryInfo{};
    const auto & effective_pushed_down_filters
        = pushed_down_filters == nullptr ? empty_pushed_down_filters : *pushed_down_filters;

    QueryProcessingStage::Enum stage;
    auto [storage, column_names, query_info] = prepareForRead(context, table_id, keep_order);
    const auto scan_column_infos = table_scan_column_infos == nullptr
        ? mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id])
        : *table_scan_column_infos;
    if (table_scan_column_infos != nullptr)
        column_names = getColumnNamesForDeltaMergeRead(table_schema_for_delta_merge[table_id], scan_column_infos);
    if (filter_conditions && filter_conditions->hasValue())
    {
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            filter_conditions->conditions,
            empty_ann_query_info,
            effective_pushed_down_filters,
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        BlockInputStreams ins = storage->read(
            column_names,
            query_info,
            context,
            stage,
            8192,
            1); // TODO: Support config max_block_size and num_streams
        // TODO: set num_streams, then ins.size() != 1
        BlockInputStreamPtr in = ins[0];
        auto analyzer = std::make_unique<DAGExpressionAnalyzer>(in->getHeader(), context);
        auto may_need_add_cast_column = getMayNeedAddCastColumnAfterTableScan(
            scan_column_infos,
            effective_pushed_down_filters,
            filter_conditions,
            context,
            /*enable_multi_stage_late_materialization=*/false);
        auto extra_cast = buildExtraCastsAfterTableScan(*analyzer, may_need_add_cast_column, scan_column_infos);
        if (extra_cast)
        {
            in = std::make_shared<ExpressionBlockInputStream>(in, extra_cast, "test");
            in->setExtraInfo("cast after table scan");
        }

        auto [before_where, filter_column_name, project_after_where]
            = analyzer->buildPushDownFilter(filter_conditions->conditions);
        in = std::make_shared<FilterBlockInputStream>(in, before_where, filter_column_name, "test");
        in->setExtraInfo("push down filter");
        in = std::make_shared<ExpressionBlockInputStream>(in, project_after_where, "test");
        in->setExtraInfo("projection after push down filter");
        return in;
    }
    else
    {
        static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_filters{};
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            empty_filters,
            empty_ann_query_info,
            effective_pushed_down_filters,
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        BlockInputStreams ins = storage->read(column_names, query_info, context, stage, 8192, 1);
        BlockInputStreamPtr in = ins[0];
        if (table_scan_column_infos != nullptr)
        {
            DAGExpressionAnalyzer analyzer{in->getHeader(), context};
            auto may_need_add_cast_column = getMayNeedAddCastColumnAfterTableScan(
                scan_column_infos,
                effective_pushed_down_filters,
                /*filter_conditions=*/nullptr,
                context,
                /*enable_multi_stage_late_materialization=*/false);
            auto extra_cast = buildExtraCastsAfterTableScan(analyzer, may_need_add_cast_column, scan_column_infos);
            if (extra_cast)
            {
                in = std::make_shared<ExpressionBlockInputStream>(in, extra_cast, "test");
                in->setExtraInfo("cast after table scan");
            }
        }
        return in;
    }
}

void MockStorage::buildExecFromDeltaMerge(
    PipelineExecutorContext & exec_context_,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    Int64 table_id,
    size_t concurrency,
    bool keep_order,
    const FilterConditions * filter_conditions,
    std::vector<int> runtime_filter_ids,
    int rf_max_wait_time_ms,
    const google::protobuf::RepeatedPtrField<tipb::Expr> * pushed_down_filters,
    const String & table_scan_executor_id,
    const TiDB::ColumnInfos * table_scan_column_infos)
{
    static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_pushed_down_filters{};
    static const auto empty_ann_query_info = tipb::ANNQueryInfo{};
    const auto & effective_pushed_down_filters
        = pushed_down_filters == nullptr ? empty_pushed_down_filters : *pushed_down_filters;

    auto [storage, column_names, query_info] = prepareForRead(context, table_id, keep_order);
    const auto scan_column_infos = table_scan_column_infos == nullptr
        ? mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id])
        : *table_scan_column_infos;
    if (table_scan_column_infos != nullptr)
        column_names = getColumnNamesForDeltaMergeRead(table_schema_for_delta_merge[table_id], scan_column_infos);
    if (filter_conditions && filter_conditions->hasValue())
    {
        const auto enable_multi_stage_late_materialization = shouldEnableMultiStageLateMaterializationForMockDeltaMerge(
            context,
            keep_order,
            filter_conditions,
            effective_pushed_down_filters,
            scan_column_infos);
        DM::MultiStageLateMaterializationRuntimeStatsPtr multi_stage_late_materialization_runtime_stats;
        if (enable_multi_stage_late_materialization)
        {
            multi_stage_late_materialization_runtime_stats
                = std::make_shared<DM::MultiStageLateMaterializationRuntimeStats>();
            if (auto * dag_context = context.getDAGContext(); dag_context != nullptr && !table_scan_executor_id.empty())
            {
                dag_context->setExecutorRowsOverride(
                    table_scan_executor_id,
                    std::shared_ptr<std::atomic<UInt64>>(
                        multi_stage_late_materialization_runtime_stats,
                        &multi_stage_late_materialization_runtime_stats->stage0_output_rows));
                dag_context->setExecutorRowsOverride(
                    filter_conditions->executor_id,
                    std::shared_ptr<std::atomic<UInt64>>(
                        multi_stage_late_materialization_runtime_stats,
                        &multi_stage_late_materialization_runtime_stats->stage1_output_rows));
            }
        }
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            filter_conditions->conditions,
            empty_ann_query_info,
            effective_pushed_down_filters,
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        query_info.enable_multi_stage_late_materialization = enable_multi_stage_late_materialization;
        query_info.multi_stage_late_materialization_runtime_stats = multi_stage_late_materialization_runtime_stats;
        storage->read(
            exec_context_,
            group_builder,
            column_names,
            query_info,
            context,
            context.getSettingsRef().max_block_size,
            concurrency);

        DAGExpressionAnalyzer analyzer{group_builder.getCurrentHeader(), context};
        auto may_need_add_cast_column = getMayNeedAddCastColumnAfterTableScan(
            scan_column_infos,
            effective_pushed_down_filters,
            filter_conditions,
            context,
            enable_multi_stage_late_materialization);
        auto extra_cast = buildExtraCastsAfterTableScan(analyzer, may_need_add_cast_column, scan_column_infos);
        executeExpression(exec_context_, group_builder, extra_cast, Logger::get("test for cast after table scan"));

        if (enable_multi_stage_late_materialization)
            return;

        // Not using `auto [before_where, filter_column_name, project_after_where]` just to make the compiler happy.
        auto build_ret = analyzer.buildPushDownFilter(filter_conditions->conditions);
        auto log = Logger::get("test for late materialization");
        auto input_header = group_builder.getCurrentHeader();
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<FilterTransformOp>(
                exec_context_,
                log->identifier(),
                input_header,
                std::get<0>(build_ret),
                std::get<1>(build_ret)));
        });
        executeExpression(exec_context_, group_builder, std::get<2>(build_ret), log);
    }
    else
    {
        static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_filters{};
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            empty_filters,
            empty_ann_query_info,
            effective_pushed_down_filters,
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        storage->read(
            exec_context_,
            group_builder,
            column_names,
            query_info,
            context,
            context.getSettingsRef().max_block_size,
            concurrency);
        if (table_scan_column_infos != nullptr)
        {
            DAGExpressionAnalyzer analyzer{group_builder.getCurrentHeader(), context};
            auto may_need_add_cast_column = getMayNeedAddCastColumnAfterTableScan(
                scan_column_infos,
                effective_pushed_down_filters,
                /*filter_conditions=*/nullptr,
                context,
                /*enable_multi_stage_late_materialization=*/false);
            auto extra_cast = buildExtraCastsAfterTableScan(analyzer, may_need_add_cast_column, scan_column_infos);
            executeExpression(exec_context_, group_builder, extra_cast, Logger::get("test for cast after table scan"));
        }
    }
}

void MockStorage::addTableInfoForDeltaMerge(const String & name, const MockColumnInfoVec & columns)
{
    TableInfo table_info;
    table_info.name = name;
    table_info.id = getTableIdForDeltaMerge(name);
    int i = 0;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo ret;
        ret.name = column.name;
        ret.tp = column.type;

        if (!column.nullable)
            ret.setNotNullFlag();
        // TODO: find a way to assign decimal field's flen.
        if (ret.tp == TiDB::TP::TypeNewDecimal)
            ret.flen = 65;
        ret.id = i++;
        table_info.columns.push_back(std::move(ret));
    }
    table_infos_for_delta_merge[name] = table_info;
}

void MockStorage::addNamesAndTypesForDeltaMerge(Int64 table_id, const ColumnsWithTypeAndName & columns)
{
    NamesAndTypes names_and_types;
    for (const auto & column : columns)
    {
        names_and_types.emplace_back(column.name, column.type);
    }
    names_and_types_map_for_delta_merge[table_id] = names_and_types;
}

Int64 MockStorage::getTableIdForDeltaMerge(const String & name)
{
    if (name_to_id_map_for_delta_merge.find(name) != name_to_id_map_for_delta_merge.end())
    {
        return name_to_id_map_for_delta_merge[name];
    }
    throw Exception(fmt::format("Failed to get table id by table name '{}'", name));
}

bool MockStorage::tableExistsForDeltaMerge(Int64 table_id)
{
    return table_schema_for_delta_merge.find(table_id) != table_schema_for_delta_merge.end();
}

MockColumnInfoVec MockStorage::getTableSchemaForDeltaMerge(const String & name)
{
    if (tableExistsForDeltaMerge(getTableIdForDeltaMerge(name)))
    {
        return table_schema_for_delta_merge[getTableIdForDeltaMerge(name)];
    }
    throw Exception(fmt::format("Failed to get table schema by table name '{}'", name));
}

MockColumnInfoVec MockStorage::getTableSchemaForDeltaMerge(Int64 table_id)
{
    if (tableExistsForDeltaMerge(table_id))
    {
        return table_schema_for_delta_merge[table_id];
    }
    throw Exception(fmt::format("Failed to get table schema by table id '{}'", table_id));
}

NamesAndTypes MockStorage::getNameAndTypesForDeltaMerge(Int64 table_id)
{
    if (tableExistsForDeltaMerge(table_id))
    {
        return names_and_types_map_for_delta_merge[table_id];
    }
    throw Exception(fmt::format("Failed to get NamesAndTypes by table id '{}'", table_id));
}

NamesAndTypes MockStorage::getNameAndTypesForDeltaMerge(Int64 table_id, const TiDB::ColumnInfos & scan_column_infos)
{
    if (tableExistsForDeltaMerge(table_id))
        return getNameAndTypesForDeltaMergeRead(table_schema_for_delta_merge[table_id], scan_column_infos);
    throw Exception(fmt::format("Failed to get NamesAndTypes by table id '{}'", table_id));
}

/// for exchange receiver
void MockStorage::addExchangeSchema(const String & exchange_name, const MockColumnInfoVec & columnInfos)
{
    exchange_schemas[exchange_name] = columnInfos;
}

void MockStorage::addExchangeData(const String & exchange_name, const ColumnsWithTypeAndName & columns)
{
    exchange_columns[exchange_name] = columns;
}

void MockStorage::addFineGrainedExchangeData(
    const String & exchange_name,
    const std::vector<ColumnsWithTypeAndName> & columns)
{
    fine_grained_exchange_columns[exchange_name] = columns;
}

bool MockStorage::exchangeExists(const String & executor_id)
{
    return exchange_schemas.find(executor_id_to_name_map[executor_id]) != exchange_schemas.end();
}

bool MockStorage::exchangeExistsWithName(const String & name)
{
    return exchange_schemas.find(name) != exchange_schemas.end();
}

std::vector<ColumnsWithTypeAndName> MockStorage::getFineGrainedExchangeColumnsVector(
    const String & executor_id,
    size_t fine_grained_stream_count)
{
    if (exchangeExists(executor_id))
    {
        auto exchange_name = executor_id_to_name_map[executor_id];
        if (fine_grained_exchange_columns.find(exchange_name) != fine_grained_exchange_columns.end())
        {
            RUNTIME_CHECK_MSG(
                fine_grained_exchange_columns[exchange_name].size() == fine_grained_stream_count,
                "Fine grained exchange data does not match fine grained stream count for exchange receiver {}",
                executor_id);
            return fine_grained_exchange_columns[exchange_name];
        }
        if (exchange_columns.find(exchange_name) != exchange_columns.end())
        {
            auto columns = exchange_columns[exchange_name];
            if (columns[0].column == nullptr || columns[0].column->empty())
                return {};
            throw Exception(
                fmt::format("Failed to get fine grained exchange columns by executor_id '{}'", executor_id));
        }
        return {};
    }
    throw Exception(fmt::format("Failed to get exchange columns by executor_id '{}'", executor_id));
}

ColumnsWithTypeAndName MockStorage::getExchangeColumns(const String & executor_id)
{
    if (exchangeExists(executor_id))
    {
        return exchange_columns[executor_id_to_name_map[executor_id]];
    }
    throw Exception(fmt::format("Failed to get exchange columns by executor_id '{}'", executor_id));
}

void MockStorage::addExchangeRelation(const String & executor_id, const String & exchange_name)
{
    executor_id_to_name_map[executor_id] = exchange_name;
}

MockColumnInfoVec MockStorage::getExchangeSchema(const String & exchange_name)
{
    if (exchangeExistsWithName(exchange_name))
    {
        return exchange_schemas[exchange_name];
    }
    throw Exception(fmt::format("Failed to get exchange schema by exchange name '{}'", exchange_name));
}

void MockStorage::clear()
{
    for (auto [_, storage] : storage_delta_merge_map)
    {
        storage->drop();
        storage->removeFromTMTContext();
    }
}

void MockStorage::setUseDeltaMerge(bool flag)
{
    use_storage_delta_merge = flag;
}

bool MockStorage::useDeltaMerge() const
{
    return use_storage_delta_merge;
}

// use this function to determine where to cut the columns,
// and how many rows are needed for each partition of MPP task.
CutColumnInfo getCutColumnInfo(size_t rows, Int64 partition_id, Int64 partition_num)
{
    int start, per_rows, rows_left, cur_rows;
    per_rows = rows / partition_num;
    rows_left = rows - per_rows * partition_num;
    if (partition_id >= rows_left)
    {
        start = (per_rows + 1) * rows_left + (partition_id - rows_left) * per_rows;
        cur_rows = per_rows;
    }
    else
    {
        start = (per_rows + 1) * partition_id;
        cur_rows = per_rows + 1;
    }
    return {start, cur_rows};
}

ColumnsWithTypeAndName getUsedColumns(
    const TiDB::ColumnInfos & used_columns,
    const ColumnsWithTypeAndName & all_columns)
{
    if (used_columns.empty())
        /// if used columns is not set, just return all the columns
        return all_columns;
    ColumnsWithTypeAndName res;
    for (const auto & column_with_type_and_name : all_columns)
    {
        bool contains = false;
        for (const auto & column : used_columns)
        {
            if (column.id == column_with_type_and_name.column_id)
            {
                contains = true;
                break;
            }
        }
        if (contains)
        {
            res.push_back(ColumnWithTypeAndName(
                column_with_type_and_name.column,
                column_with_type_and_name.type,
                column_with_type_and_name.name));
        }
    }
    return res;
}

ColumnsWithTypeAndName MockStorage::getColumnsForMPPTableScan(
    const TiDBTableScan & table_scan,
    Int64 partition_id,
    Int64 partition_num)
{
    auto table_id = table_scan.getLogicalTableID();
    if (tableExists(table_id))
    {
        auto columns_with_type_and_name = table_columns[table_scan.getLogicalTableID()];
        size_t rows = 0;
        for (const auto & col : columns_with_type_and_name)
        {
            if (rows == 0)
                rows = col.column->size();
            assert(rows == col.column->size());
        }

        CutColumnInfo cut_info = getCutColumnInfo(rows, partition_id, partition_num);

        ColumnsWithTypeAndName res = getUsedColumns(table_scan.getColumns(), columns_with_type_and_name);
        for (auto & column_with_type_and_name : res)
        {
            column_with_type_and_name.column = column_with_type_and_name.column->cut(cut_info.first, cut_info.second);
        }
        return res;
    }
    throw Exception(fmt::format("Failed to get table columns by table_id '{}'", table_id));
}

void MockStorage::addTableInfo(const String & name, const MockColumnInfoVec & columns)
{
    TableInfo table_info;
    table_info.name = name;
    table_info.id = getTableId(name);
    auto column_infos = mockColumnInfosToTiDBColumnInfos(columns);
    table_info.columns.swap(column_infos);
    table_infos[name] = table_info;
}

TableInfo MockStorage::getTableInfo(const String & name)
{
    return table_infos[name];
}

TableInfo MockStorage::getTableInfoForDeltaMerge(const String & name)
{
    return table_infos_for_delta_merge[name];
}

DM::ColumnDefines MockStorage::getStoreColumnDefines(Int64 table_id)
{
    return storage_delta_merge_map[table_id]->getStoreColumnDefines();
}

TiDB::ColumnInfos mockColumnInfosToTiDBColumnInfos(const MockColumnInfoVec & mock_column_infos)
{
    ColumnID col_id = 0;
    TiDB::ColumnInfos ret;
    ret.reserve(mock_column_infos.size());
    for (const auto & mock_column_info : mock_column_infos)
    {
        TiDB::ColumnInfo column_info;
        column_info.name = mock_column_info.name;
        column_info.tp = mock_column_info.type;
        column_info.collate = mock_column_info.collate;
        column_info.id = col_id++;
        // TODO: find a way to assign decimal field's flen.
        if (column_info.tp == TiDB::TP::TypeNewDecimal)
            column_info.flen = 65;
        if (!mock_column_info.nullable)
            column_info.setNotNullFlag();
        ret.push_back(std::move(column_info));
    }
    return ret;
}

} // namespace DB

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
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/Context.h>
#include <Operators/FilterTransformOp.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>

namespace DB
{
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
    int rf_max_wait_time_ms)
{
    static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_pushed_down_filters{};

    QueryProcessingStage::Enum stage;
    auto [storage, column_names, query_info] = prepareForRead(context, table_id, keep_order);
    if (filter_conditions && filter_conditions->hasValue())
    {
        auto analyzer = std::make_unique<DAGExpressionAnalyzer>(names_and_types_map_for_delta_merge[table_id], context);
        auto scan_column_infos = mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id]);
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            filter_conditions->conditions,
            empty_pushed_down_filters, // Not care now
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        auto [before_where, filter_column_name, project_after_where]
            = analyzer->buildPushDownFilter(filter_conditions->conditions);
        BlockInputStreams ins = storage->read(
            column_names,
            query_info,
            context,
            stage,
            8192,
            1); // TODO: Support config max_block_size and num_streams
        // TODO: set num_streams, then ins.size() != 1
        BlockInputStreamPtr in = ins[0];
        in = std::make_shared<FilterBlockInputStream>(in, before_where, filter_column_name, "test");
        in->setExtraInfo("push down filter");
        in = std::make_shared<ExpressionBlockInputStream>(in, project_after_where, "test");
        in->setExtraInfo("projection after push down filter");
        return in;
    }
    else
    {
        static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_filters{};
        auto scan_column_infos = mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id]);
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            empty_filters,
            empty_pushed_down_filters, // Not care now
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        BlockInputStreams ins = storage->read(column_names, query_info, context, stage, 8192, 1);
        BlockInputStreamPtr in = ins[0];
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
    int rf_max_wait_time_ms)
{
    static const google::protobuf::RepeatedPtrField<tipb::Expr> empty_pushed_down_filters{};

    auto [storage, column_names, query_info] = prepareForRead(context, table_id, keep_order);
    if (filter_conditions && filter_conditions->hasValue())
    {
        auto analyzer = std::make_unique<DAGExpressionAnalyzer>(names_and_types_map_for_delta_merge[table_id], context);
        auto scan_column_infos = mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id]);
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            filter_conditions->conditions,
            empty_pushed_down_filters, // Not care now
            scan_column_infos,
            runtime_filter_ids,
            rf_max_wait_time_ms,
            context.getTimezoneInfo());
        // Not using `auto [before_where, filter_column_name, project_after_where]` just to make the compiler happy.
        auto build_ret = analyzer->buildPushDownFilter(filter_conditions->conditions);
        storage->read(
            exec_context_,
            group_builder,
            column_names,
            query_info,
            context,
            context.getSettingsRef().max_block_size,
            concurrency);
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
        auto scan_column_infos = mockColumnInfosToTiDBColumnInfos(table_schema_for_delta_merge[table_id]);
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            empty_filters,
            empty_pushed_down_filters, // Not care now
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

ColumnsWithTypeAndName getUsedColumns(const ColumnInfos & used_columns, const ColumnsWithTypeAndName & all_columns)
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

ColumnInfos mockColumnInfosToTiDBColumnInfos(const MockColumnInfoVec & mock_column_infos)
{
    ColumnID col_id = 0;
    ColumnInfos ret;
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

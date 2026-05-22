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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <Storages/StorageDisaggregatedHelpers.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>
#include <common/DateLUT.h>
#include <kvproto/kvrpcpb.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <ext/scope_guard.h>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int COLUMNAR_SNAPSHOT_ERROR;
} // namespace ErrorCodes

namespace
{
std::vector<std::tuple<UInt64, String, DataTypePtr>> genGeneratedColumnInfosForDisaggregatedRead(
    const TiDBTableScan & table_scan)
{
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    generated_column_infos.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        const auto & ci = table_scan.getColumns()[i];
        if (!ci.hasGeneratedColumnFlag())
            continue;
        // Disaggregated read behaves like ExchangeReceiver output.
        generated_column_infos.emplace_back(
            static_cast<UInt64>(i),
            genNameForExchangeReceiver(i),
            getDataTypeByColumnInfoForComputingLayer(ci));
    }
    return generated_column_infos;
}

std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedReadThroughColumnar(
    const TiDBTableScan & table_scan)
{
    static auto trace_log = Logger::get("StorageDisaggregatedColumnar");
    LOG_INFO(trace_log, "[columnar_trace] genColumnDefinesForDisaggregatedReadThroughColumnar begin");
    DM::ColumnDefinesPtr column_defines;
    int extra_table_id_index;
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    try
    {
        std::tie(column_defines, extra_table_id_index, generated_column_infos)
            = genColumnDefinesForDisaggregatedRead(table_scan);
    }
    catch (const std::bad_alloc &)
    {
        LOG_ERROR(trace_log, "[columnar_trace] std::bad_alloc in genColumnDefinesForDisaggregatedRead");
        throw;
    }
    LOG_INFO(
        trace_log,
        "[columnar_trace] genColumnDefinesForDisaggregatedRead done, num_columns={}, extra_table_id_index={}",
        column_defines->size(),
        extra_table_id_index);

    // Columnar only support the legacy string format for now, so convert the data type to legacy one.
    // We can remove this when columnar supports the new string data type.
    LOG_INFO(trace_log, "[columnar_trace] before convertDataType for columnar legacy string");
    for (auto & cd : *column_defines)
    {
        const auto & converted_type = CodecUtils::convertDataType(*cd.type);
        if (&converted_type != cd.type.get())
            cd.type = DataTypeFactory::instance().getOrSet(converted_type.getName());
    }
    LOG_INFO(trace_log, "[columnar_trace] convertDataType done");

    // genColumnDefinesForDisaggregatedRead already skips generated columns.
    // executeGeneratedColumnPlaceholder fills virtual columns later in the pipeline.
    LOG_INFO(
        trace_log,
        "[columnar_trace] genColumnDefinesForDisaggregatedReadThroughColumnar end, num_columns={}, extra_table_id_index={}",
        column_defines->size(),
        extra_table_id_index);
    return {std::move(column_defines), extra_table_id_index};
}

bool isProxyFilterComparableExpr(tipb::ScalarFuncSig sig)
{
    // Keep this aligned with proxy columnar filter supported signatures:
    // `contrib/tiflash-proxy/components/kvengine/src/table/columnar/filter.rs`.
    switch (sig)
    {
    case tipb::ScalarFuncSig::LTInt:
    case tipb::ScalarFuncSig::LTReal:
    case tipb::ScalarFuncSig::LTString:
    case tipb::ScalarFuncSig::LTDecimal:
    case tipb::ScalarFuncSig::LTTime:
    case tipb::ScalarFuncSig::LTDuration:
    case tipb::ScalarFuncSig::LTJson:
    case tipb::ScalarFuncSig::LEInt:
    case tipb::ScalarFuncSig::LEReal:
    case tipb::ScalarFuncSig::LEString:
    case tipb::ScalarFuncSig::LEDecimal:
    case tipb::ScalarFuncSig::LETime:
    case tipb::ScalarFuncSig::LEDuration:
    case tipb::ScalarFuncSig::LEJson:
    case tipb::ScalarFuncSig::GTInt:
    case tipb::ScalarFuncSig::GTReal:
    case tipb::ScalarFuncSig::GTString:
    case tipb::ScalarFuncSig::GTDecimal:
    case tipb::ScalarFuncSig::GTTime:
    case tipb::ScalarFuncSig::GTDuration:
    case tipb::ScalarFuncSig::GTJson:
    case tipb::ScalarFuncSig::GEInt:
    case tipb::ScalarFuncSig::GEReal:
    case tipb::ScalarFuncSig::GEString:
    case tipb::ScalarFuncSig::GEDecimal:
    case tipb::ScalarFuncSig::GETime:
    case tipb::ScalarFuncSig::GEDuration:
    case tipb::ScalarFuncSig::GEJson:
    case tipb::ScalarFuncSig::EQInt:
    case tipb::ScalarFuncSig::EQReal:
    case tipb::ScalarFuncSig::EQString:
    case tipb::ScalarFuncSig::EQDecimal:
    case tipb::ScalarFuncSig::EQTime:
    case tipb::ScalarFuncSig::EQDuration:
    case tipb::ScalarFuncSig::EQJson:
    case tipb::ScalarFuncSig::NEInt:
    case tipb::ScalarFuncSig::NEReal:
    case tipb::ScalarFuncSig::NEString:
    case tipb::ScalarFuncSig::NEDecimal:
    case tipb::ScalarFuncSig::NETime:
    case tipb::ScalarFuncSig::NEDuration:
    case tipb::ScalarFuncSig::NEJson:
    case tipb::ScalarFuncSig::InInt:
    case tipb::ScalarFuncSig::InReal:
    case tipb::ScalarFuncSig::InString:
    case tipb::ScalarFuncSig::InDecimal:
    case tipb::ScalarFuncSig::InTime:
    case tipb::ScalarFuncSig::InDuration:
        return true;
    default:
        return false;
    }
}

void normalizeTimestampCompareDateTimeLiteralToUTC(tipb::Expr & expr, const TimezoneInfo & timezone_info)
{
    if (timezone_info.is_utc_timezone)
        return;
    if (!isFunctionExpr(expr))
        return;

    // Only normalize for comparison expressions that proxy filter supports.
    // Keep recursion so nested comparisons under AND/OR/NOT still work.
    if (isScalarFunctionExpr(expr) && isProxyFilterComparableExpr(expr.sig()))
    {
        bool has_timestamp_column = false;
        bool only_column_or_literal = true;
        size_t column_ref_count = 0;
        for (const auto & child : expr.children())
        {
            if (isColumnExpr(child))
            {
                ++column_ref_count;
                has_timestamp_column = has_timestamp_column
                    || (child.has_field_type() && child.field_type().tp() == TiDB::TypeTimestamp);
            }
            else if (!isLiteralExpr(child))
            {
                only_column_or_literal = false;
            }
        }

        // Proxy filter parser only supports simple column-literal expressions.
        // If a timestamp column is compared with a datetime literal, normalize the
        // datetime literal from session timezone to UTC before passing to proxy.
        if (has_timestamp_column && only_column_or_literal && column_ref_count == 1)
        {
            static const auto & time_zone_utc = DateLUT::instance("UTC");
            for (int i = 0; i < expr.children_size(); ++i)
            {
                auto * child = expr.mutable_children(i);
                if (!isLiteralExpr(*child) || !child->has_field_type())
                    continue;
                if (child->tp() != tipb::ExprType::MysqlTime || child->field_type().tp() != TiDB::TypeDatetime)
                    continue;

                UInt64 from_time = decodeLiteral(*child).get<UInt64>();
                UInt64 result_time = from_time;
                if (timezone_info.is_name_based)
                    convertTimeZone(from_time, result_time, *timezone_info.timezone, time_zone_utc);
                else if (timezone_info.timezone_offset != 0)
                    convertTimeZoneByOffset(from_time, result_time, false, timezone_info.timezone_offset);
                child->set_val(constructDateTimeLiteralTiExpr(result_time).val());
            }
        }
    }

    for (int i = 0; i < expr.children_size(); ++i)
        normalizeTimestampCompareDateTimeLiteralToUTC(*expr.mutable_children(i), timezone_info);
}
} // namespace

BlockInputStreams StorageDisaggregated::readThroughColumnar(const Context & context, unsigned num_streams)
{
    DAGPipeline pipeline;
    const UInt64 start_ts = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    auto read_proxy_tasks = RNProxyReadTask::buildProxyReadTaskWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    for (auto & task : read_proxy_tasks)
    {
        auto streams = task->getInputStreams();
        pipeline.streams.insert(pipeline.streams.end(), streams.begin(), streams.end());
    }
    // Avoid reading generated columns from proxy, generate placeholders locally.
    executeGeneratedColumnPlaceholder(generated_column_infos, log, pipeline);
    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto & stream_header = pipeline.firstStream()->getHeader();
    for (const auto & col : stream_header)
    {
        source_columns.emplace_back(col.name, col.type);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration/timestamp cast for proxy path.
    // We still execute pushed-down filters on RN side, so timestamp columns in those filters
    // must also be converted from UTC to session timezone.
    extraCast(*analyzer, pipeline, /*include_pushed_down_filter_columns=*/true);
    // Handle filter
    filterConditionsWithPushedDownFilters(*analyzer, pipeline);
    return pipeline.streams;
}


void StorageDisaggregated::readThroughColumnar(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const Context & context,
    unsigned num_streams)
{
    LOG_INFO(log, "[columnar_trace] readThroughColumnar(pipeline) begin");
    const UInt64 start_ts = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
    LOG_INFO(log, "[columnar_trace] buildRemoteTableRanges done, region_num={}", region_num);
    LOG_INFO(log, "[columnar_trace] before buildProxyReadTaskWithBackoff");
    auto read_proxy_tasks = RNProxyReadTask::buildProxyReadTaskWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    LOG_INFO(log, "[columnar_trace] buildProxyReadTaskWithBackoff done, task_num={}", read_proxy_tasks.size());
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedReadThroughColumnar(table_scan);
    LOG_INFO(
        log,
        "[columnar_trace] genColumnDefines done, num_columns={}, extra_table_id_index={}",
        column_defines->size(),
        extra_table_id_index);
    for (auto & task : read_proxy_tasks)
    {
        group_builder.addConcurrency(RNProxySourceOp::create({
            .context = context,
            .debug_tag = log->identifier(),
            .exec_context = exec_context,
            .columns_to_read = *column_defines,
            .task = task,
            .extra_table_id_index = extra_table_id_index,
        }));
    }
    LOG_INFO(log, "[columnar_trace] RNProxySourceOp added, concurrency={}", group_builder.concurrency());

    executeGeneratedColumnPlaceholder(exec_context, group_builder, generated_column_infos, log);
    LOG_INFO(
        log,
        "[columnar_trace] executeGeneratedColumnPlaceholder done, gen_col_num={}",
        generated_column_infos.size());

    NamesAndTypes source_columns;
    auto header = group_builder.getCurrentHeader();
    source_columns.reserve(header.columns());
    for (const auto & col : header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    LOG_INFO(log, "[columnar_trace] DAGExpressionAnalyzer created, source_columns={}", source_columns.size());

    // Handle duration/timestamp cast for proxy path.
    LOG_INFO(log, "[columnar_trace] before extraCast");
    extraCast(exec_context, group_builder, *analyzer, /*include_pushed_down_filter_columns=*/true);
    LOG_INFO(log, "[columnar_trace] extraCast done");
    // Handle filter
    LOG_INFO(
        log,
        "[columnar_trace] before filterConditionsWithPushedDownFilters, pushed_down_filter_num={}",
        table_scan.getPushedDownFilters().size());
    filterConditionsWithPushedDownFilters(exec_context, group_builder, *analyzer);
    LOG_INFO(log, "[columnar_trace] filterConditionsWithPushedDownFilters done");
    LOG_INFO(log, "[columnar_trace] readThroughColumnar(pipeline) end");
}

// RNProxyReaderPtr
RNProxyReaderPtr RNProxyReader::createProxyReader(
    const LoggerPtr & log,
    const Context & context,
    RegionID region_id,
    RegionVersion region_ver,
    UInt64 region_conf_ver,
    const std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> & physical_table_ranges,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions,
    std::mutex & output_lock)
{
    LOG_INFO(
        log,
        "[columnar_trace] createProxyReader begin region_id={}, filter_conditions_num={}, pushed_down_filter_num={}",
        region_id,
        filter_conditions.conditions.size(),
        table_scan.getPushedDownFilters().size());
    auto table_scan_pb = *table_scan.getTableScanPB();
    const auto & timezone_info = context.getTimezoneInfo();
    if (table_scan_pb.tp() == tipb::TypePartitionTableScan)
    {
        auto * pushed_down_filters
            = table_scan_pb.mutable_partition_table_scan()->mutable_pushed_down_filter_conditions();
        for (int i = 0; i < pushed_down_filters->size(); ++i)
            normalizeTimestampCompareDateTimeLiteralToUTC(*pushed_down_filters->Mutable(i), timezone_info);
    }
    else
    {
        auto * pushed_down_filters = table_scan_pb.mutable_tbl_scan()->mutable_pushed_down_filter_conditions();
        for (int i = 0; i < pushed_down_filters->size(); ++i)
            normalizeTimestampCompareDateTimeLiteralToUTC(*pushed_down_filters->Mutable(i), timezone_info);
    }
    auto table_scan_data = table_scan_pb.SerializeAsString();
    BaseBuffView table_scan_view = BaseBuffView{table_scan_data.data(), table_scan_data.size()};
    auto conditions = filter_conditions.conditions;
    for (int i = 0; i < conditions.size(); ++i)
        normalizeTimestampCompareDateTimeLiteralToUTC(*conditions.Mutable(i), timezone_info);
    // Copy pushed down filters to filter_conditions to make filterConditions works properly.
    // Proxy columnar reader use pushed down filters to reduce packs load from disk and has no
    // guarantee to filter all useless data, so we rely on the filterConditions to filter data.
    String tables_range_data;
    for (const auto & [physical_table_id, ranges] : physical_table_ranges)
    {
        tables_range_data.append(reinterpret_cast<const char *>(&physical_table_id), sizeof(physical_table_id));

        String ranges_data;
        for (const auto & range : ranges)
        {
            tipb::KeyRange range_pb;
            range_pb.set_low(range.start_key);
            range_pb.set_high(range.end_key);
            auto data = range_pb.SerializeAsString();
            uint32_t len = data.size();
            ranges_data.append(reinterpret_cast<const char *>(&len), sizeof(len));
            ranges_data.append(data.data(), data.size());
        }
        uint32_t ranges_data_size = ranges_data.size();
        tables_range_data.append(reinterpret_cast<const char *>(&ranges_data_size), sizeof(ranges_data_size));
        tables_range_data.append(ranges_data.data(), ranges_data.size());
    }
    BaseBuffView tables_range_view = BaseBuffView{tables_range_data.data(), tables_range_data.size()};
    String filter_conditions_data;
    for (const auto & condition : conditions)
    {
        auto data = condition.SerializeAsString();
        uint32_t len = data.size();
        filter_conditions_data.append(reinterpret_cast<const char *>(&len), sizeof(len));
        filter_conditions_data.append(data.data(), data.size());
    }
    tipb::TableInfo table_info;
    bool is_partition_scan = table_scan.isPartitionTableScan();
    const auto & tidbColumns = table_scan.getColumns();
    if (is_partition_scan)
    {
        for (const auto & column : table_scan_pb.partition_table_scan().columns())
        {
            const auto column_id = column.column_id();
            bool is_generated_column = false;
            for (const auto & ci : tidbColumns)
            {
                if (ci.id == column_id && ci.hasGeneratedColumnFlag())
                {
                    is_generated_column = true;
                    break;
                }
            }
            if (is_generated_column)
                continue;
            *table_info.add_columns() = column;
        }
    }
    else
    {
        for (const auto & column : table_scan_pb.tbl_scan().columns())
        {
            const auto column_id = column.column_id();
            bool is_generated_column = false;
            for (const auto & ci : tidbColumns)
            {
                if (ci.id == column_id && ci.hasGeneratedColumnFlag())
                {
                    is_generated_column = true;
                    break;
                }
            }
            if (is_generated_column)
                continue;
            *table_info.add_columns() = column;
        }
    }
    auto table_info_data = table_info.SerializeAsString();
    BaseBuffView columns = BaseBuffView{table_info_data.data(), table_info_data.size()};
    BaseBuffView filter_conditions_view = BaseBuffView{filter_conditions_data.data(), filter_conditions_data.size()};
    auto ann_query_info_pb = table_scan.getANNQueryInfo();
    auto fts_query_info_pb = table_scan.getFTSQueryInfo();
    auto ann_query_info_data = ann_query_info_pb.SerializeAsString();
    auto fts_query_info_data = fts_query_info_pb.SerializeAsString();
    BaseBuffView ann_query_info_view = BaseBuffView{ann_query_info_data.data(), ann_query_info_data.size()};
    BaseBuffView fts_query_info_view = BaseBuffView{fts_query_info_data.data(), fts_query_info_data.size()};
    const Context & global_ctx = context.getGlobalContext();
    auto * cluster = global_ctx.getTMTContext().getKVCluster();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar proxy helper is not initialized");
    ColumnarReaderPtr columnar_reader = proxy_helper->cloud_storage_engine_interfaces.fn_get_columnar_reader(
        region_id,
        region_ver,
        start_ts,
        std::move(tables_range_view),
        std::move(columns),
        std::move(table_scan_view),
        std::move(filter_conditions_view),
        std::move(ann_query_info_view),
        std::move(fts_query_info_view),
        proxy_helper->proxy_ptr);
    LOG_INFO(
        log,
        "[columnar_trace] fn_get_columnar_reader done region_id={}, error_type={}",
        region_id,
        static_cast<UInt8>(columnar_reader.error_type));
    bool reader_transferred = false;
    SCOPE_EXIT({
        if (!reader_transferred)
            RustGcHelper::instance().gcRustPtr(columnar_reader.inner.ptr, columnar_reader.inner.type);
    });
    SCOPE_EXIT({
        if (!reader_transferred && columnar_reader.error_type != ColumnarReaderErrorType::OK)
            RustGcHelper::instance().gcRustPtr(columnar_reader.error.inner.ptr, columnar_reader.error.inner.type);
    });
    if (columnar_reader.error_type == ColumnarReaderErrorType::RegionError)
    {
        auto error_msg = String(columnar_reader.error.buff.data, columnar_reader.error.buff.len);
        errorpb::Error region_error;
        region_error.ParseFromString(error_msg);
        auto region_ver_id = pingcap::kv::RegionVerID(region_id, region_conf_ver, region_ver);
        // Refresh region cache and throw an exception for retrying.
        if (region_error.has_epoch_not_match())
        {
            RegionException::UnavailableRegions unavailable_regions;
            String region_id_ver; // region_id:region_ver:conf_ver
            std::unordered_set<RegionID> retry_regions;
            for (const auto & region : region_error.epoch_not_match().current_regions())
            {
                unavailable_regions.insert(region.id());
                retry_regions.insert(region.id());
                region_id_ver = std::to_string(region.id()) + ":" + std::to_string(region_ver) + ":"
                    + std::to_string(region.region_epoch().conf_ver());
            }
            auto _guard = std::lock_guard(output_lock);
            cluster->region_cache->dropRegion(region_ver_id);
            LOG_WARNING(
                log,
                "create columnar reader failed region_id={}, epoch not match {}",
                std::to_string(region_id),
                region_ver_id.toString());
            throw RegionException(
                std::move(unavailable_regions),
                RegionException::RegionReadStatus::EPOCH_NOT_MATCH,
                region_id_ver.c_str());
        }
        else
        {
            RegionException::UnavailableRegions unavailable_regions;
            std::unordered_set<RegionID> retry_regions;
            auto err_region_id = 0;
            if (region_error.has_region_not_found())
            {
                err_region_id = region_error.region_not_found().region_id();
                unavailable_regions.insert(err_region_id);
                retry_regions.insert(err_region_id);
                LOG_WARNING(
                    log,
                    "create columnar reader failed region_id={}, region not found {}",
                    std::to_string(region_id),
                    std::to_string(err_region_id));
            }
            else
            {
                LOG_WARNING(
                    log,
                    "create columnar reader failed region_id={}, {}",
                    std::to_string(region_id),
                    region_error.ShortDebugString());
            }
            auto _guard = std::lock_guard(output_lock);
            cluster->region_cache->dropRegion(region_ver_id);
            throw RegionException(
                std::move(unavailable_regions),
                RegionException::RegionReadStatus::NOT_FOUND,
                std::to_string(region_id).c_str());
        }
    }
    else if (columnar_reader.error_type == ColumnarReaderErrorType::LockedError)
    {
        auto error_msg = String(columnar_reader.error.buff.data, columnar_reader.error.buff.len);
        kvrpcpb::LockInfo lock_info;
        lock_info.ParseFromString(error_msg);
        // Try to resolve locks.
        pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
        std::vector<uint64_t> pushed;
        std::vector<pingcap::kv::LockPtr> locks{std::make_shared<pingcap::kv::Lock>(lock_info)};
        auto _guard = std::lock_guard(output_lock);
        auto before_expired = cluster->lock_resolver->resolveLocks(bo, start_ts, locks, pushed);
        LOG_WARNING(log, "Finished resolve locks, before_expired={}", before_expired);
        throw Exception("lock error", ErrorCodes::COLUMNAR_SNAPSHOT_ERROR);
    }
    else if (columnar_reader.error_type == ColumnarReaderErrorType::PdClientError)
    {
        auto error_msg = String(columnar_reader.error.buff.data, columnar_reader.error.buff.len);
        LOG_WARNING(log, "create columnar reader failed, pd client error: {}", error_msg);
        throw Exception(fmt::format("pd client error: {}", error_msg), ErrorCodes::COLUMNAR_SNAPSHOT_ERROR);
    }
    else if (columnar_reader.error_type != ColumnarReaderErrorType::OK)
    {
        auto error_msg = String(columnar_reader.error.buff.data, columnar_reader.error.buff.len);
        LOG_WARNING(
            log,
            "create columnar reader failed, error_type={}, error={}",
            uint8_t(columnar_reader.error_type),
            error_msg);
        throw Exception(
            fmt::format("columnar reader error_type={}, error={}", uint8_t(columnar_reader.error_type), error_msg),
            ErrorCodes::COLUMNAR_SNAPSHOT_ERROR);
    }

    // Create input stream.
    LOG_INFO(log, "[columnar_trace] before genColumnDefinesForDisaggregatedReadThroughColumnar region_id={}", region_id);
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedReadThroughColumnar(table_scan);
    LOG_INFO(
        log,
        "[columnar_trace] genColumnDefinesForDisaggregatedReadThroughColumnar done region_id={}, num_columns={}, extra_table_id_index={}",
        region_id,
        column_defines->size(),
        extra_table_id_index);
    LOG_INFO(log, "[columnar_trace] before RNProxyInputStream::create region_id={}", region_id);
    BlockInputStreamPtr input_stream = RNProxyInputStream::create({
        .context = context,
        .debug_tag = log->identifier(),
        .columns_to_read = *column_defines,
        .reader = columnar_reader,
        .extra_table_id_index = extra_table_id_index,
        .table_id = table_scan.getLogicalTableID(),
        .executor_id = table_scan.getTableScanExecutorID(),
    });
    LOG_INFO(log, "[columnar_trace] RNProxyInputStream::create done region_id={}", region_id);
    reader_transferred = true;
    return std::make_shared<RNProxyReader>(input_stream);
}

// RNProxyReadTask
std::vector<RNProxyReadTaskPtr> RNProxyReadTask::buildProxyReadTaskWithBackoff(
    const LoggerPtr & log,
    const Context & context,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions,
    const std::vector<RemoteTableRange> & remote_table_ranges,
    unsigned num_streams)
{
    std::vector<RNProxyReadTaskPtr> tasks;
    pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
    while (true)
    {
        try
        {
            tasks = RNProxyReadTask::buildProxyReadTask(
                log,
                context,
                start_ts,
                table_scan,
                filter_conditions,
                remote_table_ranges,
                num_streams);
            break;
        }
        catch (RegionException & e)
        {
            LOG_WARNING(log, "buildProxyReadTask failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::COLUMNAR_SNAPSHOT_ERROR)
                throw;
            LOG_WARNING(log, "buildProxyReadTask failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
    }
    return tasks;
}

std::vector<RNProxyReadTaskPtr> RNProxyReadTask::buildProxyReadTask(
    const LoggerPtr & log,
    const Context & context,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions,
    const std::vector<RemoteTableRange> & remote_table_ranges,
    unsigned num_streams)
{
    auto * dag_context = context.getDAGContext();
    auto scan_context
        = std::make_shared<DM::ScanContext>(dag_context->getKeyspaceID(), dag_context->getResourceGroupName());
    dag_context->scan_context_map[table_scan.getTableScanExecutorID()] = scan_context;

    std::vector<RNProxyReadTaskPtr> tasks;
    // Collect all regions in the table scan.
    std::unordered_map<uint64_t, std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>>
        all_remote_regions_by_region;
    std::unordered_map<uint64_t, pingcap::kv::RegionVerID> region_ver_ids;

    std::vector<UInt64> physical_table_ids;
    std::vector<pingcap::coprocessor::KeyRanges> ranges_for_each_physical_table;
    physical_table_ids.reserve(remote_table_ranges.size());
    ranges_for_each_physical_table.reserve(remote_table_ranges.size());
    for (const auto & remote_table_range : remote_table_ranges)
    {
        physical_table_ids.emplace_back(remote_table_range.first);
        ranges_for_each_physical_table.emplace_back(remote_table_range.second);
    }
    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    auto & region_cache = cluster->region_cache;
    for (auto idx = 0; idx < int(ranges_for_each_physical_table.size()); idx++)
    {
        const auto physical_table_id = physical_table_ids[idx];
        const auto ranges = ranges_for_each_physical_table[idx];
        const auto locations = pingcap::coprocessor::details::splitKeyRangesByLocations(region_cache, bo, ranges);
        for (const auto & location : locations)
        {
            // If the region_ver_ids already exists, compare the value with location.location.region.
            // If the value is not equal, drop cache and retry.
            const auto & region = location.location.region;
            if (auto it = region_ver_ids.find(region.id); it != region_ver_ids.end() && it->second != region)
            {
                region_cache->dropRegion(it->second);
                region_cache->dropRegion(region);
                region_ver_ids.erase(it);
                LOG_WARNING(
                    log,
                    "buildProxyReadTask failed region_id={}, epoch not match {}",
                    region.id,
                    region.toString());
                throw RegionException(
                    RegionException::UnavailableRegions{region.id},
                    RegionException::RegionReadStatus::EPOCH_NOT_MATCH,
                    region.toString().c_str());
            }
            all_remote_regions_by_region[region.id].push_back(std::make_tuple(physical_table_id, location.ranges));
            region_ver_ids[region.id] = region;
            LOG_DEBUG(
                log,
                "buildProxyReadTask, physical_table_id={}, region_ver_id={}",
                physical_table_id,
                region.toString());
        }
    }
    unsigned region_num = all_remote_regions_by_region.size();
    unsigned physical_table_num = physical_table_ids.size();
    unsigned real_num_streams = std::min(num_streams, region_num);
    // Regions per RNProxyReader, it should be ceil of region_num / real_num_streams.
    // `regions_per_reader` is the ceil of the division, so the concurrency may be less than `real_num_streams`.
    unsigned regions_per_reader = (region_num + real_num_streams - 1) / real_num_streams;
    LOG_INFO(
        log,
        "region_num={}, table_num={}, num_streams={}, real_num_streams={}, regions_per_reader={}",
        region_num,
        physical_table_num,
        num_streams,
        real_num_streams,
        regions_per_reader);
    unsigned reader_idx = 0;
    std::vector<RNProxyReaderPtr> all_readers;
    std::mutex output_lock;
    auto thread_manager = newThreadManager();

    for (const auto & [region_id, physical_table_ranges] : all_remote_regions_by_region)
    {
        auto region_ver = region_ver_ids[region_id].ver;
        auto region_conf_ver = region_ver_ids[region_id].conf_ver;
        thread_manager->schedule(
            true,
            "createProxyReader",
            [log,
             &context,
             region_id,
             region_ver,
             region_conf_ver,
             physical_table_ranges,
             start_ts,
             &table_scan,
             &filter_conditions,
             &output_lock,
             &all_readers] {
                LOG_INFO(
                    log,
                    "create proxy reader for tables in region, region_id={}, table_num={}",
                    region_id,
                    physical_table_ranges.size());
                auto reader_ptr = RNProxyReader::createProxyReader(
                    log,
                    context,
                    region_id,
                    region_ver,
                    region_conf_ver,
                    physical_table_ranges,
                    start_ts,
                    table_scan,
                    filter_conditions,
                    output_lock);
                {
                    std::lock_guard lock(output_lock);
                    all_readers.push_back(reader_ptr);
                }
            });
    }

    thread_manager->wait();

    std::vector<RNProxyReaderPtr> readers;
    for (auto & reader : all_readers)
    {
        ++reader_idx;
        readers.push_back(reader);
        if (reader_idx == regions_per_reader)
        {
            reader_idx = 0;
            tasks.push_back(std::make_shared<RNProxyReadTask>(std::move(readers)));
            readers.clear();
        }
    }

    if (!readers.empty())
    {
        tasks.push_back(std::make_shared<RNProxyReadTask>(std::move(readers)));
    }

    return tasks;
}

BlockInputStreams RNProxyReadTask::getInputStreams() const
{
    BlockInputStreams streams;
    streams.reserve(proxy_readers.size());
    for (const auto & reader : proxy_readers)
    {
        streams.push_back(reader->getInputStream());
    }
    return streams;
}

// RNProxyInputStream
RNProxyInputStream::~RNProxyInputStream()
{
    SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(reader.inner.ptr, reader.inner.type); });
    try
    {
        LOG_INFO(
            log,
            "Finished reading remote snapshot through proxy, rows={} bytes={} read_cost={:.3f}s "
            "deserialize_cost={:.3f}s",
            action.totalRows(),
            total_bytes,
            duration_read_sec,
            duration_deserialize_sec);
        auto * dag_context = context.getDAGContext();
        if (auto it = dag_context->scan_context_map.find(executor_id); it != dag_context->scan_context_map.end())
        {
            if (it->second)
            {
                std::optional<LACBytesCollector> lac_bytes_collector;
                it->second->addUserReadBytes(total_bytes, DM::ReadTag::Query, lac_bytes_collector);
            }
        }
    }
    catch (...)
    {
        // Destructors must not throw.
    }
}

Block RNProxyInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}

Block RNProxyInputStream::readImpl()
{
    FilterPtr filter_ignored;
    return readImpl(filter_ignored, false);
}

Block RNProxyInputStream::readImpl([[maybe_unused]] FilterPtr & res_filter, [[maybe_unused]] bool return_filter)
{
    if (done)
        return {};
    const Context & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar proxy helper is not initialized");
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    UInt64 rows = proxy_helper->cloud_storage_engine_interfaces.fn_read_block(reader, batch_size);
    duration_read_sec += w.elapsedSecondsFromLastTime();
    LOG_DEBUG(log, "Read {} rows from proxy", rows);
    if (rows == std::numeric_limits<UInt64>::max())
    {
        LOG_WARNING(log, "Read block from proxy failed");
        throw Exception("read_block failed in tiflash-proxy", ErrorCodes::LOGICAL_ERROR);
    }
    if (rows == 0)
        return {};

    TableID physical_table_id = -1;
    Block header = getHeader();
    const ColumnsWithTypeAndName col_type_and_name = header.getColumnsWithTypeAndName();
    // Construct block from proxy column data.
    MutableColumns columns = header.cloneEmptyColumns();
    for (UInt32 i = 0; i < col_type_and_name.size(); ++i)
    {
        LOG_DEBUG(
            log,
            "Read column id={} name={} type={}",
            col_type_and_name[i].column_id,
            col_type_and_name[i].name,
            col_type_and_name[i].type->getName());
        // Read column data from proxy
        Int64 col_id = col_type_and_name[i].column_id;
        if (col_id == MutSup::extra_handle_id)
        {
            RustStrWithView col_data = proxy_helper->cloud_storage_engine_interfaces.fn_read_handle(reader);
            SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
            physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader);
            ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
            auto & col = *columns[i];
            col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                col,
                [&](const IDataType::SubstreamPath &) { return &buf; },
                rows,
                -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from proxy
                true,
                {});
        }
        else if (col_id == MutSup::extra_table_id_col_id)
        {
            continue;
        }
        else
        {
            RustStrWithView col_data = proxy_helper->cloud_storage_engine_interfaces.fn_read_column(reader, col_id);
            SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
            physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader);
            ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
            auto & col = *columns[i];
            col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                col,
                [&](const IDataType::SubstreamPath &) { return &buf; },
                rows,
                -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from proxy
                true,
                {});
            LOG_DEBUG(log, "Read column data done, col size={}", col.size());
        }
    }
    duration_deserialize_sec += w.elapsedSecondsFromLastTime();

    Block block = header.cloneWithColumns(std::move(columns));
    LOG_DEBUG(log, "Read block rows={}, structure={}", block.rows(), block.dumpStructure());
    if (physical_table_id == -1)
    {
        LOG_WARNING(log, "physical_table_id is not set, use table_id {} instead", table_id);
        physical_table_id = table_id;
    }
    // Fill extra table id column.
    action.fill(block, physical_table_id);
    block.checkNumberOfRows();

    total_bytes += block.bytes();
    return block;
}

// RNProxySourceOp
void RNProxySourceOp::operateSuffixImpl()
{
    UNUSED(context);
    LOG_INFO(log, "Finished reading proxy snapshots, rows={} cost={:.3f}s", total_rows, duration_read_sec);
}

void RNProxySourceOp::operatePrefixImpl()
{
    LOG_INFO(log, "Begin reading proxy snapshots");
}

OperatorStatus RNProxySourceOp::readImpl(Block & block)
{
    if (unlikely(done))
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }

    if (t_block.has_value())
    {
        std::swap(block, t_block.value());
        t_block.reset();
        return OperatorStatus::HAS_OUTPUT;
    }

    return current_reader_idx < 0 ? OperatorStatus::IO_IN : awaitImpl();
}

OperatorStatus RNProxySourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (unlikely(current_reader_idx < 0))
    {
        current_reader_idx = 0;
    }

    return OperatorStatus::IO_IN;
}

OperatorStatus RNProxySourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (unlikely(current_reader_idx < 0))
    {
        return awaitImpl();
    }

    LOG_INFO(
        log,
        "[columnar_trace] RNProxySourceOp::executeIOImpl begin reader_idx={}/{}",
        current_reader_idx,
        task->getProxyReaders().size());
    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block;
    try
    {
        block = task->getProxyReaders()[current_reader_idx]->getInputStream()->read(filter_ignored, false);
    }
    catch (const std::bad_alloc &)
    {
        LOG_ERROR(
            log,
            "[columnar_trace] std::bad_alloc in RNProxySourceOp::executeIOImpl reader_idx={}",
            current_reader_idx);
        throw;
    }
    LOG_INFO(log, "[columnar_trace] RNProxySourceOp::executeIOImpl read done rows={}", block.rows());
    duration_read_sec += w.elapsedSeconds();
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        t_block.emplace(std::move(block));
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        if (current_reader_idx == Int32(task->getProxyReaders().size() - 1))
        {
            done = true;
        }
        else if (current_reader_idx < Int32(task->getProxyReaders().size() - 1))
        {
            ++current_reader_idx;
        }
        // Current stream is drained, try to read from next stream.
        return awaitImpl();
    }
}

} // namespace DB
#endif

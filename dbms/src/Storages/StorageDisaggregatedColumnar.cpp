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
#include <Common/RedactHelpers.h>
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
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
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
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int COLUMNAR_SNAPSHOT_ERROR;
} // namespace ErrorCodes

struct RNProxyReaderSharedContext
{
    using ClearSharedSnapAccessByStartTsFn = void (*)(uint64_t, RaftStoreProxyPtr);

    struct StartTsClearRegistry
    {
        enum class UnregisterResult
        {
            NotRegistered,
            NotLastOwner,
            LastOwner,
        };

        std::mutex mutex;
        std::unordered_map<UInt64, UInt64> ref_counts;

        void registerStartTs(UInt64 start_ts)
        {
            if (start_ts == 0)
                return;
            auto guard = std::lock_guard(mutex);
            ++ref_counts[start_ts];
        }

        UnregisterResult unregisterStartTs(UInt64 start_ts)
        {
            if (start_ts == 0)
                return UnregisterResult::NotRegistered;

            auto guard = std::lock_guard(mutex);
            auto it = ref_counts.find(start_ts);
            if (it == ref_counts.end() || it->second == 0)
                return UnregisterResult::NotRegistered;
            --it->second;
            if (it->second != 0)
                return UnregisterResult::NotLastOwner;

            ref_counts.erase(it);
            return UnregisterResult::LastOwner;
        }
    };

    static StartTsClearRegistry & getStartTsClearRegistry()
    {
        static StartTsClearRegistry registry;
        return registry;
    }

    LoggerPtr log;
    const Context * context = nullptr;
    UInt64 start_ts = 0;
    DM::ColumnDefinesPtr column_defines;
    int extra_table_id_index = -1;
    TableID logical_table_id = 0;
    String executor_id;
    String table_scan_data;
    String filter_conditions_data;
    String table_info_data;
    String ann_query_info_data;
    String fts_query_info_data;
    RaftStoreProxyPtr proxy_ptr{};
    ClearSharedSnapAccessByStartTsFn clear_shared_snap_access_by_start_ts = nullptr;
    std::shared_ptr<std::mutex> output_lock = std::make_shared<std::mutex>();
    bool registered_for_start_ts = false;

    ~RNProxyReaderSharedContext() noexcept
    {
        if (!registered_for_start_ts)
            return;

        auto unregister_result = getStartTsClearRegistry().unregisterStartTs(start_ts);
        if (unregister_result != StartTsClearRegistry::UnregisterResult::LastOwner)
            return;

        if (proxy_ptr.inner == nullptr || clear_shared_snap_access_by_start_ts == nullptr)
            return;

        try
        {
            clear_shared_snap_access_by_start_ts(start_ts, proxy_ptr);
        }
        catch (...)
        {
            LOG_WARNING(log, "clear shared snapaccess cache failed, start_ts={}", start_ts);
        }
    }
};

size_t getRNProxySourceNum(size_t num_streams, size_t reader_count)
{
    return std::min(std::max<size_t>(1, num_streams), reader_count);
}

namespace
{
using ProxyPhysicalTableRanges = std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>;
using BucketSplitUnit = std::pair<TableID, pingcap::coprocessor::KeyRange>;

void normalizeTimestampCompareDateTimeLiteralToUTC(tipb::Expr & expr, const TimezoneInfo & timezone_info);

struct BucketSplitResult
{
    bool has_bucket_split = false;
    std::vector<BucketSplitUnit> units;
};

struct RegionReaderPlan
{
    RegionID region_id;
    pingcap::kv::RegionVerID region_ver_id;
    ProxyPhysicalTableRanges physical_table_ranges;
    std::vector<BucketSplitUnit> bucket_units;
};

bool isBucketBoundaryInsideRange(const String & bucket_key, const pingcap::coprocessor::KeyRange & range)
{
    if (bucket_key.empty())
        return false;
    if (!range.start_key.empty() && bucket_key <= range.start_key)
        return false;
    if (!range.end_key.empty() && bucket_key >= range.end_key)
        return false;
    return true;
}

BucketSplitResult splitRangesByBucketKeys(
    const ProxyPhysicalTableRanges & physical_table_ranges,
    const std::vector<String> & bucket_keys)
{
    BucketSplitResult result;
    if (bucket_keys.size() <= 2)
        return result;

    for (const auto & [table_id, ranges] : physical_table_ranges)
    {
        for (const auto & range : ranges)
        {
            String current_start = range.start_key;
            bool current_range_split = false;
            for (const auto & bucket_key : bucket_keys)
            {
                const auto decoded_bucket_key
                    = RecordKVFormat::decodeTiKVKey(TiKVKey(bucket_key.data(), bucket_key.size()));
                String normalized_bucket_key(decoded_bucket_key.data(), decoded_bucket_key.size());
                if (!isBucketBoundaryInsideRange(normalized_bucket_key, range))
                    continue;
                result.units.emplace_back(
                    table_id,
                    pingcap::coprocessor::KeyRange{current_start, normalized_bucket_key});
                current_start = std::move(normalized_bucket_key);
                current_range_split = true;
            }
            if (!range.end_key.empty() && current_start >= range.end_key)
                continue;
            result.units.emplace_back(table_id, pingcap::coprocessor::KeyRange{current_start, range.end_key});
            result.has_bucket_split = result.has_bucket_split || current_range_split;
        }
    }
    return result;
}

std::vector<String> getRegionBucketKeysFromProxy(const Context & context, RegionID region_id, UInt64 region_ver)
{
    const Context & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    if (proxy_helper == nullptr || proxy_helper->cloud_storage_engine_interfaces.fn_get_region_bucket_keys == nullptr)
        return {};

    RustStrWithViewVec bucket_keys = proxy_helper->cloud_storage_engine_interfaces.fn_get_region_bucket_keys(
        region_id,
        region_ver,
        proxy_helper->proxy_ptr);
    SCOPE_EXIT({
        if (bucket_keys.inner.ptr != nullptr)
            RustGcHelper::instance().gcRustPtr(bucket_keys.inner.ptr, bucket_keys.inner.type);
    });

    std::vector<String> res;
    res.reserve(static_cast<size_t>(bucket_keys.len));
    for (size_t i = 0; i < bucket_keys.len; ++i)
        res.emplace_back(bucket_keys.buffs[i].data, bucket_keys.buffs[i].len);
    return res;
}

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
    DM::ColumnDefinesPtr column_defines;
    int extra_table_id_index;
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    std::tie(column_defines, extra_table_id_index, generated_column_infos)
        = genColumnDefinesForDisaggregatedRead(table_scan);

    // Columnar only support the legacy string format for now, so convert the data type to legacy one.
    // We can remove this when columnar supports the new string data type.
    for (auto & cd : *column_defines)
    {
        const auto & converted_type = CodecUtils::convertDataType(*cd.type);
        if (&converted_type != cd.type.get())
            cd.type = DataTypeFactory::instance().getOrSet(converted_type.getName());
    }

    // genColumnDefinesForDisaggregatedRead already skips generated columns.
    // executeGeneratedColumnPlaceholder fills virtual columns later in the pipeline.
    return {std::move(column_defines), extra_table_id_index};
}

std::shared_ptr<RNProxyReaderSharedContext> buildProxyReaderSharedContext(
    const LoggerPtr & log,
    const Context & context,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions)
{
    auto shared_context = std::make_shared<RNProxyReaderSharedContext>();
    shared_context->log = log;
    shared_context->context = &context;
    shared_context->start_ts = start_ts;
    RNProxyReaderSharedContext::getStartTsClearRegistry().registerStartTs(start_ts);
    shared_context->registered_for_start_ts = true;
    shared_context->logical_table_id = table_scan.getLogicalTableID();
    shared_context->executor_id = table_scan.getTableScanExecutorID();
    const TiFlashRaftProxyHelper * proxy_helper
        = context.getGlobalContext().getSharedContextDisagg()->getColumnarProxyHelper();
    if (proxy_helper != nullptr)
    {
        shared_context->proxy_ptr = proxy_helper->proxy_ptr;
        shared_context->clear_shared_snap_access_by_start_ts
            = proxy_helper->cloud_storage_engine_interfaces.fn_clear_shared_snap_access_by_start_ts;
    }
    std::tie(shared_context->column_defines, shared_context->extra_table_id_index)
        = genColumnDefinesForDisaggregatedReadThroughColumnar(table_scan);

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
    shared_context->table_scan_data = table_scan_pb.SerializeAsString();

    auto conditions = filter_conditions.conditions;
    for (int i = 0; i < conditions.size(); ++i)
        normalizeTimestampCompareDateTimeLiteralToUTC(*conditions.Mutable(i), timezone_info);
    for (const auto & condition : conditions)
    {
        auto data = condition.SerializeAsString();
        uint32_t len = data.size();
        shared_context->filter_conditions_data.append(reinterpret_cast<const char *>(&len), sizeof(len));
        shared_context->filter_conditions_data.append(data.data(), data.size());
    }

    tipb::TableInfo table_info;
    bool is_partition_scan = table_scan.isPartitionTableScan();
    const auto & tidb_columns = table_scan.getColumns();
    const auto should_skip_column_for_columnar_table_info = [&](ColumnID column_id) {
        if (column_id == MutSup::extra_table_id_col_id)
            return true;
        for (const auto & ci : tidb_columns)
        {
            if (ci.id == column_id && ci.hasGeneratedColumnFlag())
                return true;
        }
        return false;
    };
    if (is_partition_scan)
    {
        for (const auto & column : table_scan_pb.partition_table_scan().columns())
        {
            if (should_skip_column_for_columnar_table_info(column.column_id()))
                continue;
            *table_info.add_columns() = column;
        }
    }
    else
    {
        for (const auto & column : table_scan_pb.tbl_scan().columns())
        {
            if (should_skip_column_for_columnar_table_info(column.column_id()))
                continue;
            *table_info.add_columns() = column;
        }
    }
    shared_context->table_info_data = table_info.SerializeAsString();
    shared_context->ann_query_info_data = table_scan.getANNQueryInfo().SerializeAsString();
    shared_context->fts_query_info_data = table_scan.getFTSQueryInfo().SerializeAsString();
    return shared_context;
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

void StorageDisaggregated::filterConditionsWithPushedDownFilters(
    DAGExpressionAnalyzer & analyzer,
    DAGPipeline & pipeline)
{
    // Proxy columnar reader uses late-materialization filters only to reduce packs loaded from disk.
    // It does not guarantee that all rows failing those filters are removed, so merge them into
    // FilterConditions and re-apply them in the TiFlash pipeline for correctness.
    FilterConditions conditions(filter_conditions.executor_id, filter_conditions.conditions);
    conditions.conditions.MergeFrom(table_scan.getPushedDownFilters());
    if (conditions.hasValue())
    {
        ::DB::executePushedDownFilter(conditions, analyzer, log, pipeline);
        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[conditions.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void StorageDisaggregated::filterConditionsWithPushedDownFilters(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    DAGExpressionAnalyzer & analyzer)
{
    // Proxy columnar reader uses late-materialization filters only to reduce packs loaded from disk.
    // It does not guarantee that all rows failing those filters are removed, so merge them into
    // FilterConditions and re-apply them in the TiFlash pipeline for correctness.
    FilterConditions conditions(filter_conditions.executor_id, filter_conditions.conditions);
    conditions.conditions.MergeFrom(table_scan.getPushedDownFilters());
    if (conditions.hasValue())
    {
        ::DB::executePushedDownFilter(exec_context, group_builder, conditions, analyzer, log);
        context.getDAGContext()->addOperatorProfileInfos(conditions.executor_id, group_builder.getCurProfileInfos());
    }
}

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
    const UInt64 start_ts = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
    auto read_proxy_tasks = RNProxyReadTask::buildProxyReadTaskWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    if (!read_proxy_tasks.empty())
    {
        auto & task_pool = read_proxy_tasks.front();
        const size_t source_num = task_pool->getSourceNum();
        LOG_INFO(
            log,
            "use shared proxy reader task pool, reader_num={}, source_num={}",
            task_pool->getReaderCount(),
            source_num);
        for (size_t i = 0; i < source_num; ++i)
        {
            group_builder.addConcurrency(RNProxySourceOp::create({
                .exec_context = exec_context,
                .task = task_pool,
            }));
        }
    }

    executeGeneratedColumnPlaceholder(exec_context, group_builder, generated_column_infos, log);

    NamesAndTypes source_columns;
    auto header = group_builder.getCurrentHeader();
    source_columns.reserve(header.columns());
    for (const auto & col : header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration/timestamp cast for proxy path.
    extraCast(exec_context, group_builder, *analyzer, /*include_pushed_down_filter_columns=*/true);
    // Handle filter
    filterConditionsWithPushedDownFilters(exec_context, group_builder, *analyzer);
}

ColumnarReaderPtr createProxyColumnarReader(
    const RNProxyReaderSharedContext & shared_context,
    const RNProxyReaderPlan & reader_plan)
{
    const auto & log = shared_context.log;
    const auto & context = *shared_context.context;
    String tables_range_data;
    for (const auto & [physical_table_id, ranges] : reader_plan.physical_table_ranges)
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
    BaseBuffView columns = BaseBuffView{shared_context.table_info_data.data(), shared_context.table_info_data.size()};
    BaseBuffView filter_conditions_view
        = BaseBuffView{shared_context.filter_conditions_data.data(), shared_context.filter_conditions_data.size()};
    BaseBuffView table_scan_view
        = BaseBuffView{shared_context.table_scan_data.data(), shared_context.table_scan_data.size()};
    BaseBuffView ann_query_info_view
        = BaseBuffView{shared_context.ann_query_info_data.data(), shared_context.ann_query_info_data.size()};
    BaseBuffView fts_query_info_view
        = BaseBuffView{shared_context.fts_query_info_data.data(), shared_context.fts_query_info_data.size()};
    const Context & global_ctx = context.getGlobalContext();
    auto * cluster = global_ctx.getTMTContext().getKVCluster();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar proxy helper is not initialized");
    ColumnarReaderPtr columnar_reader = proxy_helper->cloud_storage_engine_interfaces.fn_get_columnar_reader(
        reader_plan.region_id,
        reader_plan.region_ver,
        shared_context.start_ts,
        std::move(tables_range_view),
        std::move(columns),
        std::move(table_scan_view),
        std::move(filter_conditions_view),
        std::move(ann_query_info_view),
        std::move(fts_query_info_view),
        proxy_helper->proxy_ptr);
    bool reader_returned = false;
    SCOPE_EXIT({
        if (!reader_returned && columnar_reader.inner.ptr != nullptr)
            RustGcHelper::instance().gcRustPtr(columnar_reader.inner.ptr, columnar_reader.inner.type);
    });
    SCOPE_EXIT({
        if (!reader_returned && columnar_reader.error_type != ColumnarReaderErrorType::OK
            && columnar_reader.error.inner.ptr != nullptr)
            RustGcHelper::instance().gcRustPtr(columnar_reader.error.inner.ptr, columnar_reader.error.inner.type);
    });
    if (columnar_reader.error_type == ColumnarReaderErrorType::RegionError)
    {
        auto error_msg = String(columnar_reader.error.buff.data, columnar_reader.error.buff.len);
        errorpb::Error region_error;
        region_error.ParseFromString(error_msg);
        auto region_ver_id
            = pingcap::kv::RegionVerID(reader_plan.region_id, reader_plan.region_conf_ver, reader_plan.region_ver);
        // Refresh region cache and throw an exception for retrying.
        if (region_error.has_epoch_not_match())
        {
            RegionException::UnavailableRegions unavailable_regions;
            String region_id_ver; // region_id:region_ver:conf_ver
            for (const auto & region : region_error.epoch_not_match().current_regions())
            {
                unavailable_regions.insert(region.id());
                region_id_ver = std::to_string(region.id()) + ":" + std::to_string(reader_plan.region_ver) + ":"
                    + std::to_string(region.region_epoch().conf_ver());
            }
            auto _guard = std::lock_guard(*shared_context.output_lock);
            cluster->region_cache->dropRegion(region_ver_id);
            LOG_WARNING(
                log,
                "create columnar reader failed region_id={}, epoch not match {}",
                std::to_string(reader_plan.region_id),
                region_ver_id.toString());
            throw RegionException(
                std::move(unavailable_regions),
                RegionException::RegionReadStatus::EPOCH_NOT_MATCH,
                region_id_ver.c_str());
        }
        else
        {
            RegionException::UnavailableRegions unavailable_regions;
            auto err_region_id = 0;
            if (region_error.has_region_not_found())
            {
                err_region_id = region_error.region_not_found().region_id();
                unavailable_regions.insert(err_region_id);
                LOG_WARNING(
                    log,
                    "create columnar reader failed region_id={}, region not found {}",
                    std::to_string(reader_plan.region_id),
                    std::to_string(err_region_id));
            }
            else
            {
                LOG_WARNING(
                    log,
                    "create columnar reader failed region_id={}, {}",
                    std::to_string(reader_plan.region_id),
                    region_error.ShortDebugString());
            }
            auto _guard = std::lock_guard(*shared_context.output_lock);
            cluster->region_cache->dropRegion(region_ver_id);
            throw RegionException(
                std::move(unavailable_regions),
                RegionException::RegionReadStatus::NOT_FOUND,
                std::to_string(reader_plan.region_id).c_str());
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
        auto _guard = std::lock_guard(*shared_context.output_lock);
        auto before_expired = cluster->lock_resolver->resolveLocks(bo, shared_context.start_ts, locks, pushed);
        LOG_WARNING(log, "Finished resolve locks, before_expired={}", before_expired);
        throw Exception("lock error", ErrorCodes::COLUMNAR_SNAPSHOT_ERROR);
    }
    else if (columnar_reader.error_type == ColumnarReaderErrorType::PdClientError)
    {
        auto error_msg = fmt::format(
            "create columnar reader failed, pd client error: {}",
            String(columnar_reader.error.buff.data, columnar_reader.error.buff.len));
        LOG_WARNING(log, "{}", error_msg);
        throw Exception(ErrorCodes::COLUMNAR_SNAPSHOT_ERROR, "{}", error_msg);
    }
    else if (columnar_reader.error_type != ColumnarReaderErrorType::OK)
    {
        auto error_msg = fmt::format(
            "create columnar reader failed, error_type={} error={}",
            static_cast<uint8_t>(columnar_reader.error_type),
            String(columnar_reader.error.buff.data, columnar_reader.error.buff.len));
        LOG_WARNING(log, "{}", error_msg);
        throw Exception(ErrorCodes::COLUMNAR_SNAPSHOT_ERROR, "{}", error_msg);
    }

    reader_returned = true;
    return columnar_reader;
}

// RNProxyReadTask
RNProxyReaderSlot::~RNProxyReaderSlot()
{
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
}

RNProxyReadTask::RNProxyReadTask(
    std::vector<RNProxyReaderPlan> reader_plans_,
    size_t source_num_,
    std::shared_ptr<RNProxyReaderSharedContext> shared_reader_context_)
    : reader_plans(std::move(reader_plans_))
    , source_num(source_num_)
    , shared_reader_context(std::move(shared_reader_context_))
{
    RUNTIME_CHECK(source_num > 0);
    RUNTIME_CHECK(source_num <= reader_plans.size(), source_num, reader_plans.size());
    reader_slots.reserve(reader_plans.size());
    for (size_t i = 0; i < reader_plans.size(); ++i)
        reader_slots.emplace_back(std::make_shared<RNProxyReaderSlot>());
}

size_t RNProxyReadTask::getReaderCount() const
{
    return reader_plans.size();
}

size_t RNProxyReadTask::getSourceNum() const
{
    return source_num;
}

const Context & RNProxyReadTask::getContext() const
{
    return *shared_reader_context->context;
}

const LoggerPtr & RNProxyReadTask::getLog() const
{
    return shared_reader_context->log;
}

const DM::ColumnDefines & RNProxyReadTask::getColumnsToRead() const
{
    return *shared_reader_context->column_defines;
}

int RNProxyReadTask::getExtraTableIDIndex() const
{
    return shared_reader_context->extra_table_id_index;
}

TableID RNProxyReadTask::getLogicalTableID() const
{
    return shared_reader_context->logical_table_id;
}

const String & RNProxyReadTask::getExecutorID() const
{
    return shared_reader_context->executor_id;
}

ColumnarReaderPtr RNProxyReadTask::createColumnarReaderWithBackoff(size_t reader_index) const
{
    RUNTIME_CHECK(reader_index < reader_plans.size());
    const auto & reader_plan = reader_plans[reader_index];
    pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
    while (true)
    {
        try
        {
            LOG_INFO(
                getLog(),
                "materialize proxy reader for tables in region, region_id={}, table_num={}",
                reader_plan.region_id,
                reader_plan.physical_table_ranges.size());
            return createProxyColumnarReader(*shared_reader_context, reader_plan);
        }
        catch (RegionException & e)
        {
            LOG_WARNING(getLog(), "create proxy reader failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::COLUMNAR_SNAPSHOT_ERROR)
                throw;
            LOG_WARNING(getLog(), "create proxy reader failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
    }
}

ColumnarReaderPtr RNProxyReadTask::getOrCreateReader(size_t reader_index)
{
    RUNTIME_CHECK(reader_index < reader_slots.size());
    auto slot = reader_slots[reader_index];
    bool should_create_inline = false;
    {
        std::unique_lock lock(slot->mutex);
        switch (slot->state)
        {
        case RNProxyReaderMaterializeState::Ready:
        {
            auto reader = std::move(slot->reader);
            slot->reader.reset();
            slot->state = RNProxyReaderMaterializeState::Consumed;
            return reader.value();
        }
        case RNProxyReaderMaterializeState::Failed:
            std::rethrow_exception(slot->exception);
        case RNProxyReaderMaterializeState::Consumed:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "proxy reader {} is already consumed", reader_index);
        case RNProxyReaderMaterializeState::Creating:
            slot->cv.wait(lock, [&] { return slot->state != RNProxyReaderMaterializeState::Creating; });
            if (slot->state == RNProxyReaderMaterializeState::Ready)
            {
                auto reader = std::move(slot->reader);
                slot->reader.reset();
                slot->state = RNProxyReaderMaterializeState::Consumed;
                return reader.value();
            }
            if (slot->state == RNProxyReaderMaterializeState::Failed)
                std::rethrow_exception(slot->exception);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "proxy reader {} becomes invalid after wait, state={}",
                reader_index,
                static_cast<Int32>(slot->state));
        case RNProxyReaderMaterializeState::NotStarted:
            slot->state = RNProxyReaderMaterializeState::Creating;
            should_create_inline = true;
            break;
        }
    }

    RUNTIME_CHECK(should_create_inline);
    LOG_INFO(
        getLog(),
        "materialize proxy reader synchronously, reader_index={}, region_id={}",
        reader_index,
        reader_plans[reader_index].region_id);
    try
    {
        auto reader = createColumnarReaderWithBackoff(reader_index);
        {
            auto guard = std::lock_guard(slot->mutex);
            slot->state = RNProxyReaderMaterializeState::Consumed;
        }
        slot->cv.notify_all();
        return reader;
    }
    catch (...)
    {
        {
            auto guard = std::lock_guard(slot->mutex);
            slot->exception = std::current_exception();
            slot->state = RNProxyReaderMaterializeState::Failed;
        }
        slot->cv.notify_all();
        throw;
    }
}

void RNProxyReadTask::prefetchReader(size_t reader_index)
{
    if (reader_index >= reader_slots.size())
        return;

    std::call_once(prefetch_thread_manager_once, [&] { prefetch_thread_manager = newThreadManager(); });

    auto slot = reader_slots[reader_index];
    {
        auto guard = std::lock_guard(slot->mutex);
        if (slot->state != RNProxyReaderMaterializeState::NotStarted)
            return;
        slot->state = RNProxyReaderMaterializeState::Creating;
    }

    prefetch_thread_manager->scheduleThenDetach(
        true,
        "PrefetchRNProxyReader",
        [self = shared_from_this(), slot, reader_index] {
            LOG_INFO(
                self->getLog(),
                "materialize proxy reader asynchronously, reader_index={}, region_id={}",
                reader_index,
                self->reader_plans[reader_index].region_id);
            try
            {
                auto reader = self->createColumnarReaderWithBackoff(reader_index);
                {
                    auto guard = std::lock_guard(slot->mutex);
                    if (slot->state == RNProxyReaderMaterializeState::Consumed)
                        return;
                    slot->reader.emplace(std::move(reader));
                    slot->state = RNProxyReaderMaterializeState::Ready;
                }
            }
            catch (...)
            {
                {
                    auto guard = std::lock_guard(slot->mutex);
                    if (slot->state == RNProxyReaderMaterializeState::Consumed)
                        return;
                    slot->exception = std::current_exception();
                    slot->state = RNProxyReaderMaterializeState::Failed;
                }
            }
            slot->cv.notify_all();
        });
}

std::optional<size_t> RNProxyReadTask::tryAcquireReaderIndex()
{
    const size_t reader_index = next_reader_index.fetch_add(1, std::memory_order_relaxed);
    if (reader_index >= reader_plans.size())
        return std::nullopt;
    return reader_index;
}

BlockInputStreamPtr RNProxyReadTask::createInputStream(size_t reader_index)
{
    RUNTIME_CHECK(reader_index < reader_plans.size());
    return RNProxyInputStream::create({
        .context = getContext(),
        .log = getLog(),
        .task = shared_from_this(),
        .reader_index = reader_index,
        .columns_to_read = getColumnsToRead(),
        .extra_table_id_index = getExtraTableIDIndex(),
        .table_id = getLogicalTableID(),
        .executor_id = getExecutorID(),
    });
}

BlockInputStreamPtr RNProxyReadTask::createSharedInputStream()
{
    return RNProxyInputStream::create({
        .context = getContext(),
        .log = getLog(),
        .task = shared_from_this(),
        .reader_index = std::nullopt,
        .columns_to_read = getColumnsToRead(),
        .extra_table_id_index = getExtraTableIDIndex(),
        .table_id = getLogicalTableID(),
        .executor_id = getExecutorID(),
    });
}

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
    auto shared_reader_context = buildProxyReaderSharedContext(log, context, start_ts, table_scan, filter_conditions);

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
    for (auto idx = 0; idx < static_cast<int>(ranges_for_each_physical_table.size()); idx++)
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
    const bool enable_bucket_parallel = !table_scan.keepOrder() && num_streams > region_num;
    std::vector<RegionReaderPlan> region_reader_plans;
    region_reader_plans.reserve(region_num);
    size_t total_max_reader_num = region_num;
    size_t total_split_bucket_num = 0;
    for (const auto & [region_id, physical_table_ranges] : all_remote_regions_by_region)
    {
        RegionReaderPlan plan{
            .region_id = region_id,
            .region_ver_id = region_ver_ids[region_id],
            .physical_table_ranges = physical_table_ranges,
        };
        if (enable_bucket_parallel)
        {
            auto bucket_keys = getRegionBucketKeysFromProxy(context, region_id, plan.region_ver_id.ver);
            auto split_result = splitRangesByBucketKeys(physical_table_ranges, bucket_keys);
            if (split_result.has_bucket_split && split_result.units.size() > 1)
            {
                total_max_reader_num += split_result.units.size() - 1;
                total_split_bucket_num += split_result.units.size();
                plan.bucket_units = std::move(split_result.units);
            }
        }
        region_reader_plans.emplace_back(std::move(plan));
    }
    const size_t planned_reader_num = total_max_reader_num;
    if (enable_bucket_parallel)
    {
        LOG_INFO(log, "bucket parallel split bucket count={}", total_split_bucket_num);
    }
    LOG_INFO(
        log,
        "region_num={}, table_num={}, num_streams={}, keep_order={}, bucket_parallel={}, planned_reader_num={}, "
        "max_reader_num={}",
        region_num,
        physical_table_num,
        num_streams,
        table_scan.keepOrder(),
        enable_bucket_parallel,
        planned_reader_num,
        total_max_reader_num);

    std::vector<RNProxyReaderPlan> all_reader_plans;
    all_reader_plans.reserve(planned_reader_num);

    for (size_t i = 0; i < region_reader_plans.size(); ++i)
    {
        const auto & plan = region_reader_plans[i];
        if (plan.bucket_units.empty())
        {
            all_reader_plans.push_back(RNProxyReaderPlan{
                .region_id = plan.region_id,
                .region_ver = plan.region_ver_id.ver,
                .region_conf_ver = plan.region_ver_id.conf_ver,
                .physical_table_ranges = plan.physical_table_ranges,
            });
        }
        else
        {
            for (const auto & [table_id, range] : plan.bucket_units)
            {
                all_reader_plans.push_back(RNProxyReaderPlan{
                    .region_id = plan.region_id,
                    .region_ver = plan.region_ver_id.ver,
                    .region_conf_ver = plan.region_ver_id.conf_ver,
                    .physical_table_ranges
                    = ProxyPhysicalTableRanges{std::make_tuple(table_id, pingcap::coprocessor::KeyRanges{range})},
                });
            }
        }
    }

    if (all_reader_plans.empty())
        return tasks;
    tasks.push_back(std::make_shared<RNProxyReadTask>(
        std::move(all_reader_plans),
        getRNProxySourceNum(num_streams, planned_reader_num),
        shared_reader_context));
    return tasks;
}

BlockInputStreams RNProxyReadTask::getInputStreams()
{
    BlockInputStreams streams;
    streams.reserve(source_num);
    for (size_t worker_index = 0; worker_index < source_num; ++worker_index)
    {
        streams.push_back(createSharedInputStream());
    }
    return streams;
}

// RNProxyInputStream
bool RNProxyInputStream::ensureReader()
{
    if (reader.has_value())
        return true;

    if (fixed_reader_index.has_value())
    {
        current_reader_index = fixed_reader_index;
        reader.emplace(task->getOrCreateReader(fixed_reader_index.value()));
        return true;
    }

    auto next_reader_index = task->tryAcquireReaderIndex();
    if (!next_reader_index.has_value())
        return false;

    current_reader_index = next_reader_index;
    reader.emplace(task->getOrCreateReader(next_reader_index.value()));
    task->prefetchReader(next_reader_index.value() + 1);
    return true;
}

void RNProxyInputStream::releaseReader()
{
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
    reader.reset();
    current_reader_index.reset();
}

RNProxyInputStream::~RNProxyInputStream()
{
    SCOPE_EXIT({
        if (reader.has_value() && reader->inner.ptr != nullptr)
            RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
    });
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
        if (auto * dag_context = context.getDAGContext(); dag_context != nullptr)
        {
            if (auto it = dag_context->scan_context_map.find(executor_id); it != dag_context->scan_context_map.end())
            {
                if (it->second)
                {
                    std::optional<LACBytesCollector> lac_bytes_collector;
                    it->second->addUserReadBytes(total_bytes, DM::ReadTag::Query, lac_bytes_collector);
                }
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

    while (true)
    {
        if (!ensureReader())
        {
            done = true;
            return {};
        }

        Stopwatch w{CLOCK_MONOTONIC_COARSE};
        UInt64 rows = proxy_helper->cloud_storage_engine_interfaces.fn_read_block(reader.value(), batch_size);
        duration_read_sec += w.elapsedSecondsFromLastTime();
        LOG_DEBUG(log, "Read {} rows from proxy", rows);
        if (rows == std::numeric_limits<UInt64>::max())
        {
            LOG_WARNING(log, "Read block from proxy failed");
            throw Exception("read_block failed in tiflash-proxy", ErrorCodes::LOGICAL_ERROR);
        }
        if (rows == 0)
        {
            releaseReader();
            if (fixed_reader_index.has_value())
            {
                done = true;
                return {};
            }
            continue;
        }

        TableID physical_table_id = -1;
        Block header = getHeader();
        const ColumnsWithTypeAndName & col_type_and_name = header.getColumnsWithTypeAndName();
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
                RustStrWithView col_data = proxy_helper->cloud_storage_engine_interfaces.fn_read_handle(reader.value());
                SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
                physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
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
                RustStrWithView col_data
                    = proxy_helper->cloud_storage_engine_interfaces.fn_read_column(reader.value(), col_id);
                SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
                physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
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
}

// RNProxySourceOp
void RNProxySourceOp::operateSuffixImpl()
{
    UNUSED(context);
    const double total_cost_sec = total_cost_watch.elapsedSeconds();
    const UInt64 rows_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_rows) / total_cost_sec) : 0;
    const UInt64 bytes_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_bytes) / total_cost_sec) : 0;
    LOG_INFO(
        log,
        "Finished reading proxy snapshots, task_pool_worker_total_cost={:.3f}s claimed_streams={} rows={} "
        "rows_per_sec={} "
        "bytes={} bytes_per_sec={} read_cost={:.3f}s",
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void RNProxySourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
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

    return awaitImpl();
}

OperatorStatus RNProxySourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    return OperatorStatus::IO_IN;
}

OperatorStatus RNProxySourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (!current_input_stream)
    {
        auto next_reader_idx = task->tryAcquireReaderIndex();
        if (!next_reader_idx.has_value())
        {
            done = true;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_reader_idx = next_reader_idx;
        current_input_stream = task->createInputStream(current_reader_idx.value());
        ++total_streams;
        task->prefetchReader(current_reader_idx.value() + 1);
    }

    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block = current_input_stream->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        total_bytes += block.bytes();
        t_block.emplace(std::move(block));
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        current_input_stream.reset();
        current_reader_idx.reset();
        return awaitImpl();
    }
}

} // namespace DB
#endif

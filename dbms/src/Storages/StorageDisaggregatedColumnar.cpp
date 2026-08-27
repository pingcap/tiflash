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
#include <Common/MemoryTracker.h>
#include <Common/MyTime.h>
#include <Common/RedactHelpers.h>
#include <Common/Stopwatch.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/FilterTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/ColumnarScanContext.h>
#include <Flash/Coprocessor/DAGCodec.h>
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
#include <IO/Buffer/WriteBufferFromString.h>
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

#include <algorithm>
#include <ext/scope_guard.h>
#include <limits>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int COLUMNAR_SNAPSHOT_ERROR;
} // namespace ErrorCodes

struct RNColumnarReaderSharedContext
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
    google::protobuf::RepeatedPtrField<tipb::Expr> exact_filter_conditions;
    bool has_pushed_down_filter_conditions = false;
    std::vector<TiDB::ColumnInfo> scan_columns;
    String table_info_data;
    String ann_query_info_data;
    String fts_query_info_data;
    RaftStoreProxyPtr proxy_ptr{};
    ClearSharedSnapAccessByStartTsFn clear_shared_snap_access_by_start_ts = nullptr;
    std::shared_ptr<std::mutex> output_lock = std::make_shared<std::mutex>();
    bool registered_for_start_ts = false;
    // Reader capability is checked independently for every region reader. Report each state
    // at most once so an empty region cannot hide a later enabled reader without producing one
    // INFO line for every region in a large scan.
    std::atomic_bool late_materialization_enabled_logged{false};
    std::atomic_bool late_materialization_disabled_logged{false};

    ~RNColumnarReaderSharedContext() noexcept
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

size_t getRNColumnarSourceNum(size_t num_streams, size_t reader_count)
{
    return std::min(std::max<size_t>(1, num_streams), reader_count);
}

namespace
{
using ColumnarPhysicalTableRanges = std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>;
using BucketSplitUnit = std::pair<TableID, pingcap::coprocessor::KeyRange>;

constexpr UInt32 COLUMNAR_LATE_MATERIALIZATION_ABI_VERSION = 1;

const ColumnarLateMaterializationInterfaces * getLateMaterializationInterfaces()
{
#if defined(__GNUC__) || defined(__clang__)
    if (tiflash_columnar_get_late_materialization_interfaces == nullptr)
        return nullptr;
    const auto * interfaces = tiflash_columnar_get_late_materialization_interfaces();
    if (interfaces == nullptr || interfaces->version != COLUMNAR_LATE_MATERIALIZATION_ABI_VERSION
        || interfaces->size < sizeof(ColumnarLateMaterializationInterfaces)
        || interfaces->fn_read_early_block == nullptr || interfaces->fn_read_early_column == nullptr
        || interfaces->fn_materialize_selected == nullptr || interfaces->fn_read_late_column == nullptr
        || interfaces->fn_finish_materialized_block == nullptr
        || interfaces->fn_discard_late_materialization_batch == nullptr
        || interfaces->fn_is_late_materialization_supported == nullptr)
        return nullptr;
    return interfaces;
#else
    return nullptr;
#endif
}

void checkRustStrWithView(const RustStrWithView & value, const char * function_name)
{
    if (value.buff.data == nullptr && value.buff.len == 0 && value.inner.ptr == nullptr && value.inner.type == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} returned a default RustStrWithView", function_name);
}

void remapColumnRefsForLateMaterialization(
    tipb::Expr & expr,
    const std::vector<TiDB::ColumnInfo> & scan_columns,
    const std::unordered_map<ColumnID, size_t> & early_column_indexes)
{
    if (expr.tp() == tipb::ExprType::ColumnRef)
    {
        const auto column_id = getColumnIDForColumnExpr(expr, scan_columns);
        const auto it = early_column_indexes.find(column_id);
        if (it == early_column_indexes.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Late-materialization predicate column {} is absent from the early projection",
                column_id);

        WriteBufferFromOwnString buffer;
        encodeDAGInt64(static_cast<Int64>(it->second), buffer);
        expr.set_val(buffer.releaseStr());
    }

    for (int i = 0; i < expr.children_size(); ++i)
        remapColumnRefsForLateMaterialization(*expr.mutable_children(i), scan_columns, early_column_indexes);
}

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
    ColumnarPhysicalTableRanges physical_table_ranges;
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
    const ColumnarPhysicalTableRanges & physical_table_ranges,
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
                String normalized_bucket_key;
                try
                {
                    // Bucket boundaries from PD are TiKV encoded keys. Empty region boundaries and
                    // malformed non-empty keys are both possible invalid split points, and length
                    // checks alone cannot validate TiKV memcomparable encoding markers/padding.
                    // Skip only the bad boundary so the original range is still covered by a
                    // coarser reader plan.
                    const auto decoded_bucket_key
                        = RecordKVFormat::decodeTiKVKey(TiKVKey(bucket_key.data(), bucket_key.size()));
                    normalized_bucket_key.assign(decoded_bucket_key.data(), decoded_bucket_key.size());
                }
                catch (...)
                {
                    continue;
                }
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

std::vector<String> getRegionBucketKeysFromColumnar(const Context & context, RegionID region_id, UInt64 region_ver)
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

std::vector<RegionReaderPlan> buildRegionReaderPlansFromPhysicalTableRanges(
    const LoggerPtr & log,
    const Context & context,
    const ColumnarPhysicalTableRanges & physical_table_ranges)
{
    std::vector<RegionReaderPlan> region_reader_plans;
    if (physical_table_ranges.empty())
        return region_reader_plans;

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    auto & region_cache = cluster->region_cache;

    std::unordered_map<uint64_t, size_t> plan_index_by_region_id;
    region_reader_plans.reserve(physical_table_ranges.size());

    for (const auto & [physical_table_id, ranges] : physical_table_ranges)
    {
        const auto locations = pingcap::coprocessor::details::splitKeyRangesByLocations(region_cache, bo, ranges);
        for (const auto & location : locations)
        {
            const auto & region = location.location.region;
            auto it = plan_index_by_region_id.find(region.id);
            if (it == plan_index_by_region_id.end())
            {
                plan_index_by_region_id.emplace(region.id, region_reader_plans.size());
                region_reader_plans.push_back(RegionReaderPlan{
                    .region_id = region.id,
                    .region_ver_id = region,
                    .physical_table_ranges
                    = ColumnarPhysicalTableRanges{std::make_tuple(physical_table_id, location.ranges)},
                });
                continue;
            }

            auto & plan = region_reader_plans[it->second];
            if (plan.region_ver_id != region)
            {
                region_cache->dropRegion(plan.region_ver_id);
                region_cache->dropRegion(region);
                LOG_WARNING(
                    log,
                    "build RegionReaderPlan failed region_id={}, epoch not match {}",
                    region.id,
                    region.toString());
                throw RegionException(
                    RegionException::UnavailableRegions{region.id},
                    RegionException::RegionReadStatus::EPOCH_NOT_MATCH,
                    region.toString().c_str());
            }
            plan.physical_table_ranges.push_back(std::make_tuple(physical_table_id, location.ranges));
        }
    }

    return region_reader_plans;
}

std::vector<RNColumnarReaderPlan> buildReaderPlansFromRegionReaderPlans(
    const std::vector<RegionReaderPlan> & region_reader_plans)
{
    std::vector<RNColumnarReaderPlan> reader_plans;
    reader_plans.reserve(region_reader_plans.size());
    for (const auto & plan : region_reader_plans)
    {
        reader_plans.push_back(RNColumnarReaderPlan{
            .region_id = plan.region_id,
            .region_ver = plan.region_ver_id.ver,
            .region_conf_ver = plan.region_ver_id.conf_ver,
            .physical_table_ranges = plan.physical_table_ranges,
        });
    }
    return reader_plans;
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

std::shared_ptr<RNColumnarReaderSharedContext> buildColumnarReaderSharedContext(
    const LoggerPtr & log,
    const Context & context,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions)
{
    auto shared_context = std::make_shared<RNColumnarReaderSharedContext>();
    shared_context->log = log;
    shared_context->context = &context;
    shared_context->start_ts = start_ts;
    RNColumnarReaderSharedContext::getStartTsClearRegistry().registerStartTs(start_ts);
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
    shared_context->scan_columns = table_scan.getColumns();

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
    shared_context->exact_filter_conditions = conditions;
    const auto & pushed_down_filters = table_scan_pb.tp() == tipb::TypePartitionTableScan
        ? table_scan_pb.partition_table_scan().pushed_down_filter_conditions()
        : table_scan_pb.tbl_scan().pushed_down_filter_conditions();
    shared_context->has_pushed_down_filter_conditions = !pushed_down_filters.empty();
    shared_context->exact_filter_conditions.MergeFrom(pushed_down_filters);

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

bool isColumnarFilterComparableExpr(tipb::ScalarFuncSig sig)
{
    // Keep this aligned with kvengine columnar filter supported signatures:
    // `components/kvengine/src/table/columnar/filter.rs`.
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

    // Only normalize for comparison expressions that columnar filter supports.
    // Keep recursion so nested comparisons under AND/OR/NOT still work.
    if (isScalarFunctionExpr(expr) && isColumnarFilterComparableExpr(expr.sig()))
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

        // Columnar filter parser only supports simple column-literal expressions.
        // If a timestamp column is compared with a datetime literal, normalize the
        // datetime literal from session timezone to UTC before passing to columnar.
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
    // Columnar reader uses late-materialization filters only to reduce packs loaded from disk.
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
    // Columnar reader uses late-materialization filters only to reduce packs loaded from disk.
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
    auto read_columnar_tasks = RNColumnarReadTask::buildColumnarReadTaskWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    for (auto & task : read_columnar_tasks)
    {
        auto streams = task->getInputStreams();
        pipeline.streams.insert(pipeline.streams.end(), streams.begin(), streams.end());
    }
    // Avoid reading generated columns from columnar, generate placeholders locally.
    executeGeneratedColumnPlaceholder(generated_column_infos, log, pipeline);
    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto & stream_header = pipeline.firstStream()->getHeader();
    for (const auto & col : stream_header)
    {
        source_columns.emplace_back(col.name, col.type);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration/timestamp cast for columnar path.
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
    auto read_columnar_tasks = RNColumnarReadTask::buildColumnarReadTaskWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    if (!read_columnar_tasks.empty())
    {
        auto & task_pool = read_columnar_tasks.front();
        const size_t source_num = task_pool->getSourceNum();
        LOG_INFO(
            log,
            "use shared columnar reader task pool, reader_num={}, source_num={}",
            task_pool->getReaderCount(),
            source_num);
        for (size_t i = 0; i < source_num; ++i)
        {
            group_builder.addConcurrency(RNColumnarSourceOp::create({
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

    // Handle duration/timestamp cast for columnar path.
    extraCast(exec_context, group_builder, *analyzer, /*include_pushed_down_filter_columns=*/true);
    // Handle filter
    filterConditionsWithPushedDownFilters(exec_context, group_builder, *analyzer);
}

ColumnarReaderPtr createColumnarReader(
    const RNColumnarReaderSharedContext & shared_context,
    const RNColumnarReaderPlan & reader_plan)
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
    auto tables_range_view = BaseBuffView{tables_range_data.data(), tables_range_data.size()};
    auto columns = BaseBuffView{shared_context.table_info_data.data(), shared_context.table_info_data.size()};
    auto filter_conditions_view
        = BaseBuffView{shared_context.filter_conditions_data.data(), shared_context.filter_conditions_data.size()};
    auto table_scan_view = BaseBuffView{shared_context.table_scan_data.data(), shared_context.table_scan_data.size()};
    auto ann_query_info_view
        = BaseBuffView{shared_context.ann_query_info_data.data(), shared_context.ann_query_info_data.size()};
    auto fts_query_info_view
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
            auto guard = std::lock_guard(*shared_context.output_lock);
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
            auto guard = std::lock_guard(*shared_context.output_lock);
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
        std::vector<pingcap::kv::LockPtr> locks{makeLockForDisaggResolve(lock_info)};
        auto guard = std::lock_guard(*shared_context.output_lock);
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

// RNColumnarReadTask
RNColumnarReaderWork::~RNColumnarReaderWork()
{
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
}

RNColumnarReadTask::RNColumnarReadTask(
    std::vector<RNColumnarReaderPlan> reader_plans,
    size_t source_num_,
    std::shared_ptr<RNColumnarReaderSharedContext> shared_reader_context_)
    : reader_count(reader_plans.size())
    , source_num(source_num_)
    , has_multi_table_reader_plan(std::any_of(
          reader_plans.begin(),
          reader_plans.end(),
          [](const auto & reader_plan) { return reader_plan.physical_table_ranges.size() > 1; }))
    , shared_reader_context(std::move(shared_reader_context_))
{
    RUNTIME_CHECK(source_num > 0);
    RUNTIME_CHECK(source_num <= reader_count, source_num, reader_count);
    for (auto & reader_plan : reader_plans)
        pending_reader_works.push_back(std::make_shared<RNColumnarReaderWork>(std::move(reader_plan)));
}

size_t RNColumnarReadTask::getReaderCount() const
{
    return reader_count;
}

size_t RNColumnarReadTask::getSourceNum() const
{
    return source_num;
}

const Context & RNColumnarReadTask::getContext() const
{
    return *shared_reader_context->context;
}

const LoggerPtr & RNColumnarReadTask::getLog() const
{
    return shared_reader_context->log;
}

const DM::ColumnDefines & RNColumnarReadTask::getColumnsToRead() const
{
    return *shared_reader_context->column_defines;
}

int RNColumnarReadTask::getExtraTableIDIndex() const
{
    return shared_reader_context->extra_table_id_index;
}

TableID RNColumnarReadTask::getLogicalTableID() const
{
    return shared_reader_context->logical_table_id;
}

const String & RNColumnarReadTask::getExecutorID() const
{
    return shared_reader_context->executor_id;
}

google::protobuf::RepeatedPtrField<tipb::Expr> RNColumnarReadTask::getLateMaterializationFilterConditions(
    const Block & early_block) const
{
    std::unordered_map<ColumnID, size_t> early_column_indexes;
    early_column_indexes.reserve(early_block.columns());
    for (size_t index = 0; index < early_block.columns(); ++index)
    {
        const auto [it, inserted] = early_column_indexes.emplace(early_block.getByPosition(index).column_id, index);
        if (!inserted)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Late-materialization early projection contains duplicate column ID {} at indexes {} and {}",
                it->first,
                it->second,
                index);
    }

    auto conditions = shared_reader_context->exact_filter_conditions;
    for (int i = 0; i < conditions.size(); ++i)
        remapColumnRefsForLateMaterialization(
            *conditions.Mutable(i),
            shared_reader_context->scan_columns,
            early_column_indexes);
    return conditions;
}

std::unordered_set<ColumnID> RNColumnarReadTask::getExactFilterColumnIDs() const
{
    std::unordered_set<ColumnID> column_ids;
    for (const auto & condition : shared_reader_context->exact_filter_conditions)
        getColumnIDsFromExpr(condition, shared_reader_context->scan_columns, column_ids);
    return column_ids;
}

std::unordered_set<ColumnID> RNColumnarReadTask::getLateMaterializationEarlyColumnIDs() const
{
    auto column_ids = getExactFilterColumnIDs();
    column_ids.insert(MutSup::extra_handle_id);
    column_ids.insert(MutSup::version_col_id);
    for (const auto & column : shared_reader_context->scan_columns)
    {
        if (column.hasPriKeyFlag())
            column_ids.insert(column.id);
    }
    return column_ids;
}

bool RNColumnarReadTask::isLateMaterializationFilterEligible(String * reason) const
{
    const auto setReason = [reason](const char * value) {
        if (reason != nullptr)
            *reason = value;
    };

    if (has_multi_table_reader_plan)
    {
        setReason("multi_table_reader_plan");
        LOG_DEBUG(
            shared_reader_context->log,
            "Columnar late materialization filter is ineligible: reason=multi_table_reader_plan, executor_id={}, "
            "table_id={}",
            getExecutorID(),
            getLogicalTableID());
        return false;
    }
    if (!shared_reader_context->has_pushed_down_filter_conditions
        || shared_reader_context->exact_filter_conditions.empty())
    {
        setReason("no_pushed_down_filter");
        LOG_DEBUG(
            shared_reader_context->log,
            "Columnar late materialization filter is ineligible: reason=no_pushed_down_filter, "
            "has_pushed_down_filter={}, exact_filter_conditions={}, executor_id={}, table_id={}",
            shared_reader_context->has_pushed_down_filter_conditions,
            shared_reader_context->exact_filter_conditions.size(),
            getExecutorID(),
            getLogicalTableID());
        return false;
    }
    const auto column_ids = getExactFilterColumnIDs();
    if (column_ids.find(MutSup::extra_table_id_col_id) != column_ids.end())
    {
        setReason("filter_uses_extra_table_id");
        LOG_DEBUG(
            shared_reader_context->log,
            "Columnar late materialization filter is ineligible: reason=filter_uses_extra_table_id, executor_id={}, "
            "table_id={}",
            getExecutorID(),
            getLogicalTableID());
        return false;
    }
    for (const auto & column : shared_reader_context->scan_columns)
    {
        if (column_ids.find(column.id) == column_ids.end())
            continue;
        if (column.hasGeneratedColumnFlag())
        {
            setReason("filter_uses_generated_column");
            LOG_DEBUG(
                shared_reader_context->log,
                "Columnar late materialization filter is ineligible: reason=filter_uses_generated_column, "
                "column_id={}, executor_id={}, table_id={}",
                column.id,
                getExecutorID(),
                getLogicalTableID());
            return false;
        }
        const bool needs_timezone_cast
            = !shared_reader_context->context->getTimezoneInfo().is_utc_timezone && column.tp == TiDB::TypeTimestamp;
        if (needs_timezone_cast || column.tp == TiDB::TypeTime)
        {
            setReason("unsupported_filter_column_type");
            LOG_DEBUG(
                shared_reader_context->log,
                "Columnar late materialization filter is ineligible: reason=unsupported_filter_column_type, "
                "column_id={}, timezone_cast={}, executor_id={}, table_id={}",
                column.id,
                needs_timezone_cast,
                getExecutorID(),
                getLogicalTableID());
            return false;
        }
    }
    setReason("eligible");
    LOG_DEBUG(
        shared_reader_context->log,
        "Columnar late materialization filter is eligible: filter_columns={}, exact_filter_conditions={}, "
        "executor_id={}, table_id={}",
        column_ids.size(),
        shared_reader_context->exact_filter_conditions.size(),
        getExecutorID(),
        getLogicalTableID());
    return true;
}

bool RNColumnarReadTask::shouldLogLateMaterialization(bool enabled)
{
    auto & logged = enabled ? shared_reader_context->late_materialization_enabled_logged
                            : shared_reader_context->late_materialization_disabled_logged;
    return !logged.exchange(true);
}

void RNColumnarReadTask::replaceReaderWork(
    const RNColumnarReaderWorkPtr & reader_work,
    std::vector<RNColumnarReaderPlan> replanned_reader_plans)
{
    RUNTIME_CHECK(reader_work != nullptr);
    RUNTIME_CHECK(!replanned_reader_plans.empty());

    reader_work->plan = std::move(replanned_reader_plans.front());
    if (replanned_reader_plans.size() == 1)
        return;

    // If the original range now spans multiple regions, enqueue the remaining partitions for
    // other sources. These ranges are produced by re-splitting the failed work's own key ranges.
    auto queue_guard = std::lock_guard(pending_reader_works_mutex);
    for (auto it = replanned_reader_plans.rbegin(); it != replanned_reader_plans.rend() - 1; ++it)
        pending_reader_works.push_front(std::make_shared<RNColumnarReaderWork>(*it));
}

#ifdef DBMS_PUBLIC_GTEST
void RNColumnarReadTask::replaceReaderWorkForTest(
    const RNColumnarReaderWorkPtr & reader_work,
    std::vector<RNColumnarReaderPlan> replanned_reader_plans)
{
    replaceReaderWork(reader_work, std::move(replanned_reader_plans));
}
#endif

ColumnarReaderPtr RNColumnarReadTask::createColumnarReaderWithBackoff(const RNColumnarReaderWorkPtr & reader_work)
{
    RUNTIME_CHECK(reader_work != nullptr);
    pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
    while (true)
    {
        try
        {
            const auto & reader_plan = reader_work->plan;
            LOG_INFO(
                getLog(),
                "materialize columnar reader for tables in region, region_id={}, table_num={}",
                reader_plan.region_id,
                reader_plan.physical_table_ranges.size());
            return createColumnarReader(*shared_reader_context, reader_plan);
        }
        catch (RegionException & e)
        {
            if (e.status == RegionException::RegionReadStatus::EPOCH_NOT_MATCH
                || e.status == RegionException::RegionReadStatus::NOT_FOUND)
            {
                try
                {
                    // Replan only the key ranges owned by this failed work. Dropping the stale
                    // region cache happens before this exception, so this locate pass can pick up
                    // the latest region epoch and split layout.
                    auto replanned_region_reader_plans = buildRegionReaderPlansFromPhysicalTableRanges(
                        getLog(),
                        getContext(),
                        reader_work->plan.physical_table_ranges);
                    auto replanned_reader_plans = buildReaderPlansFromRegionReaderPlans(replanned_region_reader_plans);
                    const auto replanned_reader_plan_count = replanned_reader_plans.size();
                    replaceReaderWork(reader_work, std::move(replanned_reader_plans));
                    LOG_WARNING(
                        getLog(),
                        "replanned columnar reader work after region error, old_error={}, new_region_id={}, "
                        "split_count={}",
                        e.message(),
                        reader_work->plan.region_id,
                        replanned_reader_plan_count);
                }
                catch (const std::exception & replan_e)
                {
                    LOG_WARNING(getLog(), "replan columnar reader work failed, {}", replan_e.what());
                }
            }
            LOG_WARNING(getLog(), "create columnar reader failed, backoff and retry, {}", e.message());
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

ColumnarReaderPtr RNColumnarReadTask::getOrCreateReader(const RNColumnarReaderWorkPtr & reader_work)
{
    RUNTIME_CHECK(reader_work != nullptr);

    bool should_create_inline = false;
    while (true)
    {
        {
            std::unique_lock lock(reader_work->mutex);
            switch (reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
            {
                auto reader = std::move(reader_work->reader);
                reader_work->reader.reset();
                reader_work->exception = nullptr;
                reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                return reader.value();
            }
            case RNColumnarReaderMaterializeState::Failed:
                std::rethrow_exception(reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    reader_work->plan.region_id);
            case RNColumnarReaderMaterializeState::Creating:
                reader_work->cv.wait(lock, [&] {
                    return reader_work->state != RNColumnarReaderMaterializeState::Creating;
                });
                continue;
            case RNColumnarReaderMaterializeState::NotStarted:
                reader_work->state = RNColumnarReaderMaterializeState::Creating;
                should_create_inline = true;
                break;
            }
        }
        break;
    }

    RUNTIME_CHECK(should_create_inline);
    try
    {
        auto reader = createColumnarReaderWithBackoff(reader_work);
        {
            auto guard = std::lock_guard(reader_work->mutex);
            reader_work->reader.reset();
            reader_work->exception = nullptr;
            reader_work->state = RNColumnarReaderMaterializeState::Consumed;
        }
        reader_work->cv.notify_all();
        return reader;
    }
    catch (...)
    {
        {
            auto guard = std::lock_guard(reader_work->mutex);
            reader_work->reader.reset();
            reader_work->exception = std::current_exception();
            reader_work->state = RNColumnarReaderMaterializeState::Failed;
        }
        reader_work->cv.notify_all();
        throw;
    }
}

void RNColumnarReadTask::prefetchPendingWork()
{
    RNColumnarReaderWorkPtr reader_work;
    {
        auto guard = std::lock_guard(pending_reader_works_mutex);
        if (pending_reader_works.empty())
            return;
        reader_work = pending_reader_works.front();
    }

    prefetchReaderWork(reader_work);
}

void RNColumnarReadTask::prefetchReaderWork(const RNColumnarReaderWorkPtr & reader_work)
{
    RUNTIME_CHECK(reader_work != nullptr);

    {
        auto guard = std::lock_guard(reader_work->mutex);
        if (reader_work->state != RNColumnarReaderMaterializeState::NotStarted)
            return;
        reader_work->state = RNColumnarReaderMaterializeState::Creating;
    }

    LOG_INFO(getLog(), "materialize columnar reader asynchronously, region_id={}", reader_work->plan.region_id);
    newThreadManager()->scheduleThenDetach(true, "PrefetchRNColumnarReader", [self = shared_from_this(), reader_work] {
        try
        {
            auto reader = self->createColumnarReaderWithBackoff(reader_work);
            {
                auto guard = std::lock_guard(reader_work->mutex);
                if (reader_work->state == RNColumnarReaderMaterializeState::Consumed)
                    return;
                reader_work->reader.emplace(std::move(reader));
                reader_work->exception = nullptr;
                reader_work->state = RNColumnarReaderMaterializeState::Ready;
            }
        }
        catch (...)
        {
            {
                auto guard = std::lock_guard(reader_work->mutex);
                if (reader_work->state == RNColumnarReaderMaterializeState::Consumed)
                    return;
                reader_work->reader.reset();
                reader_work->exception = std::current_exception();
                reader_work->state = RNColumnarReaderMaterializeState::Failed;
            }
        }
        reader_work->cv.notify_all();
    });
}

std::optional<RNColumnarReaderWorkPtr> RNColumnarReadTask::tryAcquireReaderWork()
{
    RNColumnarReaderWorkPtr reader_work;
    {
        auto guard = std::lock_guard(pending_reader_works_mutex);
        if (pending_reader_works.empty())
            return std::nullopt;
        reader_work = pending_reader_works.front();
        pending_reader_works.pop_front();
    }
    prefetchPendingWork();
    return reader_work;
}

BlockInputStreamPtr RNColumnarReadTask::createInputStream(const RNColumnarReaderWorkPtr & reader_work)
{
    RUNTIME_CHECK(reader_work != nullptr);
    return RNColumnarInputStream::create({
        .context = getContext(),
        .log = getLog(),
        .task = shared_from_this(),
        .reader_work = reader_work,
        .columns_to_read = getColumnsToRead(),
        .extra_table_id_index = getExtraTableIDIndex(),
        .table_id = getLogicalTableID(),
        .executor_id = getExecutorID(),
    });
}

BlockInputStreamPtr RNColumnarReadTask::createSharedInputStream()
{
    return RNColumnarInputStream::create({
        .context = getContext(),
        .log = getLog(),
        .task = shared_from_this(),
        .reader_work = nullptr,
        .columns_to_read = getColumnsToRead(),
        .extra_table_id_index = getExtraTableIDIndex(),
        .table_id = getLogicalTableID(),
        .executor_id = getExecutorID(),
    });
}

std::vector<RNColumnarReadTaskPtr> RNColumnarReadTask::buildColumnarReadTaskWithBackoff(
    const LoggerPtr & log,
    const Context & context,
    UInt64 start_ts,
    const TiDBTableScan & table_scan,
    const FilterConditions & filter_conditions,
    const std::vector<RemoteTableRange> & remote_table_ranges,
    unsigned num_streams)
{
    std::vector<RNColumnarReadTaskPtr> tasks;
    pingcap::kv::Backoffer bo(pingcap::kv::copNextMaxBackoff);
    while (true)
    {
        try
        {
            tasks = RNColumnarReadTask::buildColumnarReadTask(
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
            LOG_WARNING(log, "buildColumnarReadTask failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::COLUMNAR_SNAPSHOT_ERROR)
                throw;
            LOG_WARNING(log, "buildColumnarReadTask failed, backoff and retry, {}", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
    }
    return tasks;
}

std::vector<RNColumnarReadTaskPtr> RNColumnarReadTask::buildColumnarReadTask(
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
    auto shared_reader_context
        = buildColumnarReaderSharedContext(log, context, start_ts, table_scan, filter_conditions);

    std::vector<RNColumnarReadTaskPtr> tasks;
    ColumnarPhysicalTableRanges physical_table_ranges;
    physical_table_ranges.reserve(remote_table_ranges.size());
    for (const auto & remote_table_range : remote_table_ranges)
        physical_table_ranges.emplace_back(remote_table_range.first, remote_table_range.second);

    auto region_reader_plans = buildRegionReaderPlansFromPhysicalTableRanges(log, context, physical_table_ranges);
    const auto region_num = static_cast<unsigned>(region_reader_plans.size());
    const auto physical_table_num = static_cast<unsigned>(physical_table_ranges.size());
    const bool enable_bucket_parallel = !table_scan.keepOrder() && num_streams > region_num;
    size_t total_max_reader_num = region_num;
    for (auto & plan : region_reader_plans)
    {
        if (enable_bucket_parallel)
        {
            auto bucket_keys = getRegionBucketKeysFromColumnar(context, plan.region_id, plan.region_ver_id.ver);
            auto split_result = splitRangesByBucketKeys(plan.physical_table_ranges, bucket_keys);
            if (split_result.has_bucket_split && split_result.units.size() > 1)
            {
                total_max_reader_num += split_result.units.size() - 1;
                plan.bucket_units = std::move(split_result.units);
            }
        }
    }
    LOG_INFO(
        log,
        "region_num={}, table_num={}, num_streams={}, keep_order={}, bucket_parallel={}, planned_reader_num={}",
        region_num,
        physical_table_num,
        num_streams,
        table_scan.keepOrder(),
        enable_bucket_parallel,
        total_max_reader_num);

    auto columnar_scan_context = std::make_shared<ColumnarScanContext>();
    columnar_scan_context->regions = region_num;
    columnar_scan_context->read_tasks = total_max_reader_num;
    columnar_scan_context->physical_tables = physical_table_num;
    columnar_scan_context->columns
        = shared_reader_context->column_defines != nullptr ? shared_reader_context->column_defines->size() : 0;
    dag_context->columnar_scan_context_map[table_scan.getTableScanExecutorID()] = columnar_scan_context;

    std::vector<RNColumnarReaderPlan> all_reader_plans;
    all_reader_plans.reserve(total_max_reader_num);

    for (const auto & plan : region_reader_plans)
    {
        if (plan.bucket_units.empty())
        {
            all_reader_plans.push_back(RNColumnarReaderPlan{
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
                all_reader_plans.push_back(RNColumnarReaderPlan{
                    .region_id = plan.region_id,
                    .region_ver = plan.region_ver_id.ver,
                    .region_conf_ver = plan.region_ver_id.conf_ver,
                    .physical_table_ranges
                    = ColumnarPhysicalTableRanges{std::make_tuple(table_id, pingcap::coprocessor::KeyRanges{range})},
                });
            }
        }
    }

    if (all_reader_plans.empty())
        return tasks;
    tasks.push_back(std::make_shared<RNColumnarReadTask>(
        std::move(all_reader_plans),
        getRNColumnarSourceNum(num_streams, total_max_reader_num),
        shared_reader_context));
    return tasks;
}

BlockInputStreams RNColumnarReadTask::getInputStreams()
{
    BlockInputStreams streams;
    streams.reserve(source_num);
    for (size_t worker_index = 0; worker_index < source_num; ++worker_index)
    {
        streams.push_back(createSharedInputStream());
    }
    return streams;
}

// RNColumnarInputStream
bool RNColumnarInputStream::ensureReader()
{
    if (reader.has_value())
        return true;

    if (fixed_reader_work != nullptr)
    {
        current_reader_work = fixed_reader_work;
        reader.emplace(task->getOrCreateReader(fixed_reader_work));
        initializeLateMaterialization();
        return true;
    }

    auto next_reader_work = task->tryAcquireReaderWork();
    if (!next_reader_work.has_value())
        return false;

    current_reader_work = next_reader_work.value();
    reader.emplace(task->getOrCreateReader(next_reader_work.value()));
    initializeLateMaterialization();
    return true;
}

void RNColumnarInputStream::initializeLateMaterialization()
{
    if (late_materialization_initialized)
        return;
    late_materialization_initialized = true;
    const bool setting_enabled = context.getSettingsRef().enable_columnar_l2_late_materialization;
    bool filter_eligible = false;
    String disable_reason = setting_enabled ? "filter_ineligible" : "setting_disabled";
    if (setting_enabled)
        filter_eligible = task->isLateMaterializationFilterEligible(&disable_reason);

    LOG_DEBUG(
        log,
        "Columnar late materialization prerequisites: setting_enabled={}, filter_eligible={}, executor_id={}, "
        "table_id={}",
        setting_enabled,
        filter_eligible,
        executor_id,
        table_id);

    if (setting_enabled && filter_eligible)
    {
        const auto early_column_ids = task->getLateMaterializationEarlyColumnIDs();
        size_t late_column_count = 0;
        size_t early_column_count = 0;
        for (const auto & column : header)
        {
            if (column.column_id == MutSup::extra_table_id_col_id)
                continue;
            if (early_column_ids.find(column.column_id) == early_column_ids.end())
                ++late_column_count;
            else
                ++early_column_count;
        }
        const auto early_without_system_columns = std::max<size_t>(1, early_column_count - 2);
        const auto late_to_early_ratio
            = static_cast<double>(late_column_count) / static_cast<double>(early_without_system_columns);
        if (late_column_count > 0
            && late_to_early_ratio > context.getSettingsRef().columnar_l2_late_materialization_min_late_to_early_ratio)
        {
            const auto * interfaces = getLateMaterializationInterfaces();
            if (interfaces != nullptr)
            {
                const std::vector<Int64> encoded_ids(early_column_ids.begin(), early_column_ids.end());
                const bool supported = interfaces->fn_is_late_materialization_supported(
                    reader.value(),
                    BaseBuffView{
                        reinterpret_cast<const char *>(encoded_ids.data()),
                        encoded_ids.size() * sizeof(Int64)});
                if (supported)
                {
                    late_materialization_interfaces = interfaces;
                    disable_reason = "enabled";
                    LOG_DEBUG(
                        log,
                        "Columnar late materialization enabled after reader support check: executor_id={}, table_id={}",
                        executor_id,
                        table_id);
                }
                else
                {
                    disable_reason = "reader_unsupported";
                    LOG_DEBUG(
                        log,
                        "Columnar late materialization is unavailable for this reader: executor_id={}, table_id={}",
                        executor_id,
                        table_id);
                }
            }
            else
            {
                disable_reason = "interfaces_unavailable";
                LOG_DEBUG(
                    log,
                    "Columnar late materialization is unavailable: interfaces missing or ABI incompatible, "
                    "executor_id={}, table_id={}",
                    executor_id,
                    table_id);
            }
        }
        else
        {
            disable_reason = late_column_count == 0 ? "no_late_columns" : "late_to_early_ratio_below_threshold";
            LOG_DEBUG(
                log,
                "Columnar late materialization skipped by column ratio: late_columns={}, early_columns={}, "
                "late_to_early_ratio={:.3f}, min_ratio={:.3f}, executor_id={}, table_id={}",
                late_column_count,
                early_column_count,
                late_to_early_ratio,
                context.getSettingsRef().columnar_l2_late_materialization_min_late_to_early_ratio,
                executor_id,
                table_id);
        }
        LOG_DEBUG(
            log,
            "Columnar late materialization eligibility: late_columns={}, early_columns={}, late_to_early_ratio={:.3f}, "
            "min_ratio={:.3f}",
            late_column_count,
            early_column_count,
            late_to_early_ratio,
            context.getSettingsRef().columnar_l2_late_materialization_min_late_to_early_ratio);
    }
    else if (!setting_enabled)
    {
        LOG_DEBUG(
            log,
            "Columnar late materialization skipped: setting enable_columnar_l2_late_materialization is false, "
            "executor_id={}, table_id={}",
            executor_id,
            table_id);
    }

    if (task->shouldLogLateMaterialization(late_materialization_interfaces != nullptr))
        LOG_INFO(
            log,
            "Columnar late materialization enabled={}, scope=reader, reason={}, setting_enabled={}, "
            "filter_eligible={}, executor_id={}, table_id={}",
            late_materialization_interfaces != nullptr,
            disable_reason,
            setting_enabled,
            filter_eligible,
            executor_id,
            table_id);
}

void RNColumnarInputStream::releaseReader()
{
    mergeReaderStats();
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
    reader.reset();
    current_reader_work.reset();
    late_materialization_interfaces = nullptr;
    late_materialization_filter_action.reset();
    late_materialization_initialized = false;
    late_materialization_probed = false;
}

void RNColumnarInputStream::mergeReaderStats()
{
    if (!reader.has_value() || reader->inner.ptr == nullptr)
        return;

    const auto * dag_context = context.getDAGContext();
    if (dag_context == nullptr)
        return;

    auto scan_ctx_iter = dag_context->columnar_scan_context_map.find(executor_id);
    if (scan_ctx_iter == dag_context->columnar_scan_context_map.end() || !scan_ctx_iter->second)
        return;

    const auto & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    if (proxy_helper == nullptr || proxy_helper->cloud_storage_engine_interfaces.fn_columnar_scan_stats == nullptr)
        return;

    const auto stats = proxy_helper->cloud_storage_engine_interfaces.fn_columnar_scan_stats(reader.value());
    scan_ctx_iter->second->merge(stats);
}

RNColumnarInputStream::~RNColumnarInputStream()
{
    SCOPE_EXIT({
        try
        {
            releaseReader();
        }
        catch (...)
        {}
    });
    try
    {
        const auto * dag_context = context.getDAGContext();
        const auto keyspace_id = dag_context != nullptr ? dag_context->getKeyspaceID() : NullspaceID;
        LOG_INFO(
            log,
            "Finished reading remote snapshot through columnar, keyspace_id={} rows={} bytes={} read_cost={:.3f}s "
            "deserialize_cost={:.3f}s",
            keyspace_id,
            action.totalRows(),
            total_bytes,
            duration_read_sec,
            duration_deserialize_sec);
        if (dag_context != nullptr)
        {
            if (auto it = dag_context->scan_context_map.find(executor_id); it != dag_context->scan_context_map.end())
            {
                if (it->second)
                {
                    std::optional<LACBytesCollector> lac_bytes_collector;
                    it->second->addUserReadBytes(total_bytes, DM::ReadTag::Query, lac_bytes_collector);
                }
            }
            if (auto it = dag_context->columnar_scan_context_map.find(executor_id);
                it != dag_context->columnar_scan_context_map.end() && it->second)
            {
                it->second->addUserReadBytes(total_bytes);
                it->second->addDeserializeBlockNs(static_cast<uint64_t>(duration_deserialize_sec * 1000000000.0));
            }
        }
    }
    catch (...)
    {
        // Destructors must not throw.
    }
}

Block RNColumnarInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}

Block RNColumnarInputStream::readImpl()
{
    FilterPtr filter_ignored;
    return readImpl(filter_ignored, false);
}

Block RNColumnarInputStream::readLateMaterializedBlock()
{
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    const auto early_column_ids = task->getLateMaterializationEarlyColumnIDs();
    const std::vector<Int64> encoded_ids(early_column_ids.begin(), early_column_ids.end());
    UInt64 batch_id = 0;
    TableID physical_table_id = -1;
    const UInt64 rows = late_materialization_interfaces->fn_read_early_block(
        reader.value(),
        batch_size,
        BaseBuffView{reinterpret_cast<const char *>(encoded_ids.data()), encoded_ids.size() * sizeof(Int64)},
        &batch_id,
        &physical_table_id);
    bool pending_late_materialization = rows != 0 && rows != std::numeric_limits<UInt64>::max();
    SCOPE_EXIT({
        if (pending_late_materialization)
            late_materialization_interfaces->fn_discard_late_materialization_batch(reader.value(), batch_id);
    });
    duration_read_sec += w.elapsedSecondsFromLastTime();
    LOG_DEBUG(log, "Read {} rows from columnar", rows);
    if (rows == std::numeric_limits<UInt64>::max())
    {
        LOG_WARNING(log, "Read block from columnar failed");
        throw Exception("read_block failed in columnar", ErrorCodes::LOGICAL_ERROR);
    }
    if (rows == 0)
    {
        releaseReader();
        done = fixed_reader_work != nullptr;
        return {};
    }

    // Check RSS pressure once the columnar reader has materialized a non-empty block,
    // before deserializing more column data into TiFlash memory.
    CurrentMemoryTracker::checkRssLimit();

    Block header = getHeader();
    const ColumnsWithTypeAndName & col_type_and_name = header.getColumnsWithTypeAndName();
    Block early_block;
    for (const auto & column : col_type_and_name)
    {
        if (column.column_id == MutSup::extra_table_id_col_id
            || early_column_ids.find(column.column_id) == early_column_ids.end())
            continue;
        (void)w.elapsedSecondsFromLastTime();
        auto col_data
            = late_materialization_interfaces->fn_read_early_column(reader.value(), batch_id, column.column_id);
        duration_read_sec += w.elapsedSecondsFromLastTime();
        SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
        checkRustStrWithView(col_data, "fn_read_early_column");
        ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
        auto mutable_column = column.type->createColumn();
        column.type->deserializeBinaryBulkWithMultipleStreams(
            *mutable_column,
            [&](const IDataType::SubstreamPath &) { return &buf; },
            rows,
            -1.0,
            true,
            {});
        duration_deserialize_sec += w.elapsedSecondsFromLastTime();
        early_block.insert(
            ColumnWithTypeAndName{std::move(mutable_column), column.type, column.name, column.column_id});
    }

    if (!late_materialization_filter_action)
    {
        // The filter expression and action graph are invariant for one input
        // stream. Building them for every 10K-row batch makes LM spend most of
        // its time in planner setup instead of row filtering.
        Block filter_header = early_block.cloneEmpty();
        NamesAndTypes early_names_and_types;
        early_names_and_types.reserve(filter_header.columns());
        for (const auto & column : filter_header)
            early_names_and_types.emplace_back(column.name, column.type);
        DAGExpressionAnalyzer lm_analyzer(std::move(early_names_and_types), context);
        auto filter_conditions = task->getLateMaterializationFilterConditions(filter_header);
        auto filter_actions = lm_analyzer.buildPushDownFilter(filter_conditions, true);
        late_materialization_filter_action = std::make_unique<FilterTransformAction>(
            filter_header,
            std::get<0>(filter_actions),
            std::get<1>(filter_actions));
    }
    auto & filter_action = *late_materialization_filter_action;
    Block evaluation_block = early_block;
    FilterPtr selection = nullptr;
    bool any_selected = !filter_action.alwaysFalse();
    if (any_selected)
    {
        any_selected = filter_action.transform(evaluation_block, selection, true);
        if (!evaluation_block || evaluation_block.rows() == 0)
            any_selected = false;
    }

    UInt8 selection_kind = 0;
    const char * selection_data = nullptr;
    uint64_t selection_size = 0;
    if (!any_selected)
        selection_kind = 1;
    else if (selection != nullptr)
    {
        selection_kind = 2;
        selection_data = reinterpret_cast<const char *>(selection->data());
        selection_size = selection->size();
    }
    const auto selected_rows = late_materialization_interfaces->fn_materialize_selected(
        reader.value(),
        batch_id,
        selection_kind,
        BaseBuffView{selection_data, selection_size});
    if (selected_rows == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "materialize selected rows for batch {} failed", batch_id);

    if (!late_materialization_probed)
    {
        late_materialization_probed = true;
        const auto skip_ratio = rows == 0 ? 0.0 : 1.0 - static_cast<double>(selected_rows) / static_cast<double>(rows);
        const auto min_skip_ratio = context.getSettingsRef().columnar_l2_late_materialization_min_selection_skip_ratio;
        if (skip_ratio < min_skip_ratio)
        {
            if (late_materialization_interfaces->fn_discard_late_materialization_batch(reader.value(), batch_id) == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "discard LM probe batch {} failed", batch_id);
            pending_late_materialization = false;
            late_materialization_interfaces = nullptr;
            LOG_INFO(
                log,
                "Disable columnar late materialization after probe: rows={}, selected_rows={}, skip_ratio={:.4f}, "
                "min_skip_ratio={:.4f}",
                rows,
                selected_rows,
                skip_ratio,
                min_skip_ratio);
            const auto * proxy_helper = context.getGlobalContext().getSharedContextDisagg()->getColumnarProxyHelper();
            RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar helper is not initialized");
            // The probe consumed an independent LM batch. Continue immediately with the
            // legacy reader so the caller does not mistake the discarded probe for EOF.
            return readLegacyBlock(proxy_helper);
        }
        LOG_DEBUG(
            log,
            "Columnar late materialization probe: rows={}, selected_rows={}, skip_ratio={:.4f}, min_skip_ratio={:.4f}",
            rows,
            selected_rows,
            skip_ratio,
            min_skip_ratio);
    }

    if (selected_rows == 0)
    {
        if (late_materialization_interfaces->fn_discard_late_materialization_batch(reader.value(), batch_id) == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "discard empty late-materialization batch {} failed", batch_id);
        pending_late_materialization = false;
        // Account for early deserialization, exact filtering, selection, and
        // discard even when this batch produces no output rows.
        duration_deserialize_sec += w.elapsedSecondsFromLastTime();
        return {};
    }

    // Exact filter evaluation and selection are post-read processing and
    // therefore belong to deserialize_cost.
    duration_deserialize_sec += w.elapsedSecondsFromLastTime();

    MutableColumns columns = header.cloneEmptyColumns();
    for (size_t i = 0; i < col_type_and_name.size(); ++i)
    {
        const auto & column = col_type_and_name[i];
        if (column.column_id == MutSup::extra_table_id_col_id)
            continue;
        if (early_column_ids.find(column.column_id) != early_column_ids.end())
        {
            ColumnPtr value = early_block.getByName(column.name).column;
            if (!any_selected)
                value = value->cut(0, 0);
            else if (selection != nullptr)
                value = value->filter(*selection, selected_rows);
            columns[i] = value->assumeMutable();
        }
    }
    // Account for filtering/copying the early columns before measuring the
    // late-column callbacks individually.
    duration_deserialize_sec += w.elapsedSecondsFromLastTime();

    for (size_t i = 0; i < col_type_and_name.size(); ++i)
    {
        const auto & column = col_type_and_name[i];
        if (column.column_id == MutSup::extra_table_id_col_id
            || early_column_ids.find(column.column_id) != early_column_ids.end())
            continue;
        // A late-column callback may perform L2 pack IO, decompression and Rust
        // serialization. Keep that work in read_cost; only the C++ decode below
        // belongs to deserialize_cost.
        (void)w.elapsedSecondsFromLastTime();
        auto col_data
            = late_materialization_interfaces->fn_read_late_column(reader.value(), batch_id, column.column_id);
        duration_read_sec += w.elapsedSecondsFromLastTime();
        SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
        checkRustStrWithView(col_data, "fn_read_late_column");
        ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
        column.type->deserializeBinaryBulkWithMultipleStreams(
            *columns[i],
            [&](const IDataType::SubstreamPath &) { return &buf; },
            selected_rows,
            -1.0,
            true,
            {});
        duration_deserialize_sec += w.elapsedSecondsFromLastTime();
    }
    if (late_materialization_interfaces->fn_finish_materialized_block(reader.value(), batch_id) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "finish late-materialization batch {} failed", batch_id);
    pending_late_materialization = false;
    duration_deserialize_sec += w.elapsedSecondsFromLastTime();

    Block block = header.cloneWithColumns(std::move(columns));
    action.fill(block, physical_table_id == -1 ? table_id : physical_table_id);
    block.setRSResult(DM::RSResult::All);
    block.checkNumberOfRows();
    total_bytes += block.bytes();
    return block;
}

Block RNColumnarInputStream::readLegacyBlock(const TiFlashRaftProxyHelper * proxy_helper)
{
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    TableID physical_table_id = -1;
    const UInt64 rows = proxy_helper->cloud_storage_engine_interfaces.fn_read_block(reader.value(), batch_size);
    duration_read_sec += w.elapsedSecondsFromLastTime();
    LOG_DEBUG(log, "Read {} rows from columnar", rows);
    if (rows == std::numeric_limits<UInt64>::max())
    {
        LOG_WARNING(log, "Read block from columnar failed");
        throw Exception("read_block failed in columnar", ErrorCodes::LOGICAL_ERROR);
    }
    if (rows == 0)
    {
        releaseReader();
        if (fixed_reader_work != nullptr)
            done = true;
        return {};
    }

    // Check RSS pressure once the columnar reader has materialized a non-empty block,
    // before deserializing more column data into TiFlash memory.
    CurrentMemoryTracker::checkRssLimit();

    Block header = getHeader();
    const ColumnsWithTypeAndName & col_type_and_name = header.getColumnsWithTypeAndName();
    // Construct block from columnar column data.
    MutableColumns columns = header.cloneEmptyColumns();
    for (UInt32 i = 0; i < col_type_and_name.size(); ++i)
    {
        LOG_DEBUG(
            log,
            "Read column id={} name={} type={}",
            col_type_and_name[i].column_id,
            col_type_and_name[i].name,
            col_type_and_name[i].type->getName());
        // Read column data from columnar
        Int64 col_id = col_type_and_name[i].column_id;
        if (col_id == MutSup::extra_handle_id)
        {
            (void)w.elapsedSecondsFromLastTime();
            RustStrWithView col_data = proxy_helper->cloud_storage_engine_interfaces.fn_read_handle(reader.value());
            duration_read_sec += w.elapsedSecondsFromLastTime();
            SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
            physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
            ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
            auto & col = *columns[i];
            col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                col,
                [&](const IDataType::SubstreamPath &) { return &buf; },
                rows,
                -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from columnar
                true,
                {});
            duration_deserialize_sec += w.elapsedSecondsFromLastTime();
        }
        else if (col_id == MutSup::extra_table_id_col_id)
        {
            continue;
        }
        else
        {
            (void)w.elapsedSecondsFromLastTime();
            RustStrWithView col_data
                = proxy_helper->cloud_storage_engine_interfaces.fn_read_column(reader.value(), col_id);
            duration_read_sec += w.elapsedSecondsFromLastTime();
            SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
            physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
            ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
            auto & col = *columns[i];
            col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                col,
                [&](const IDataType::SubstreamPath &) { return &buf; },
                rows,
                -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from columnar
                true,
                {});
            duration_deserialize_sec += w.elapsedSecondsFromLastTime();
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

Block RNColumnarInputStream::readImpl([[maybe_unused]] FilterPtr & res_filter, [[maybe_unused]] bool return_filter)
{
    if (done)
        return {};
    const Context & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar helper is not initialized");

    while (true)
    {
        if (!ensureReader())
        {
            done = true;
            return {};
        }

        if (late_materialization_interfaces != nullptr)
        {
            Block block = readLateMaterializedBlock();
            if (block || done)
                return block;
            continue;
        }

        Block block = readLegacyBlock(proxy_helper);
        if (block || done)
            return block;
    }
}

// RNColumnarSourceOp
void RNColumnarSourceOp::operateSuffixImpl()
{
    UNUSED(context);
    const auto keyspace_id = exec_context.getKeyspaceID();
    const double total_cost_sec = total_cost_watch.elapsedSeconds();
    const UInt64 rows_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_rows) / total_cost_sec) : 0;
    const UInt64 bytes_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_bytes) / total_cost_sec) : 0;
    LOG_INFO(
        log,
        "Finished reading columnar snapshots, keyspace_id={} task_pool_worker_total_cost={:.3f}s claimed_streams={} "
        "rows={} "
        "rows_per_sec={} "
        "bytes={} bytes_per_sec={} read_cost={:.3f}s",
        keyspace_id,
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void RNColumnarSourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
    LOG_INFO(log, "Begin reading columnar snapshots, keyspace_id={}", exec_context.getKeyspaceID());
}

OperatorStatus RNColumnarSourceOp::readImpl(Block & block)
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

OperatorStatus RNColumnarSourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    return OperatorStatus::IO_IN;
}

OperatorStatus RNColumnarSourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (!current_input_stream)
    {
        auto next_reader_work = task->tryAcquireReaderWork();
        if (!next_reader_work.has_value())
        {
            done = true;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_input_stream = task->createInputStream(next_reader_work.value());
        ++total_streams;
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
        return awaitImpl();
    }
}

} // namespace DB
#endif

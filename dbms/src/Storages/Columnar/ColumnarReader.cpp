// Copyright 2026 PingCAP, Inc.
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
#include <Storages/Columnar/ColumnarReader.h>
#include <Storages/Columnar/ColumnarScanContext.h>
#include <Storages/Columnar/ColumnarStreams.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <Storages/SelectQueryInfo.h>
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

size_t getRNColumnarSourceNum(size_t num_streams, size_t reader_count)
{
    return std::min(std::max<size_t>(1, num_streams), reader_count);
}

namespace
{
using ColumnarPhysicalTableRanges = std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>;
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
        std::vector<pingcap::kv::LockPtr> locks{std::make_shared<pingcap::kv::Lock>(lock_info)};
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

} // namespace DB
#endif

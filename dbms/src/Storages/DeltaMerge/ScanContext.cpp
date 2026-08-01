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

#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop
#include <Poco/UUIDGenerator.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <magic_enum.hpp>

namespace DB::DM
{
namespace
{
constexpr uint64_t NS_TO_MS = 1'000'000;

bool isQueryReadTag(ReadTag read_tag)
{
    switch (read_tag)
    {
    case ReadTag::Query:
    case ReadTag::LMFilter:
    case ReadTag::MSLMPushedFilter:
    case ReadTag::MSLMCandidate:
    case ReadTag::MSLMFinalRest:
        return true;
    default:
        return false;
    }
}

void serializeReadStage(const MSLMReadStageScanContext & read_stage, tipb::TiFlashMSLMReadStageContext * proto)
{
    proto->set_dmfile_scanned_rows(read_stage.dmfile_scanned_rows);
    proto->set_dmfile_skipped_rows(read_stage.dmfile_skipped_rows);
    proto->set_read_bytes(read_stage.read_bytes);
    proto->set_read_time_ms(read_stage.read_time_ns / NS_TO_MS);
}

void mergeReadStage(MSLMReadStageScanContext & read_stage, const tipb::TiFlashMSLMReadStageContext & proto)
{
    read_stage.dmfile_scanned_rows += proto.dmfile_scanned_rows();
    read_stage.dmfile_skipped_rows += proto.dmfile_skipped_rows();
    read_stage.read_bytes += proto.read_bytes();
    read_stage.read_time_ns += proto.read_time_ms() * NS_TO_MS;
}

void serializeFilter(
    uint64_t input_rows,
    uint64_t selected_rows,
    uint64_t filtered_rows,
    tipb::TiFlashMSLMFilterContext * proto)
{
    proto->set_input_rows(input_rows);
    proto->set_selected_rows(selected_rows);
    proto->set_filtered_rows(filtered_rows);
}

Poco::JSON::Object::Ptr readStageToJson(const MSLMReadStageScanContext & read_stage)
{
    static constexpr double NS_TO_MS_SCALE = 1'000'000.0;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("dmfile_scanned_rows", read_stage.dmfile_scanned_rows.load());
    json->set("dmfile_skipped_rows", read_stage.dmfile_skipped_rows.load());
    json->set("read_bytes", read_stage.read_bytes.load());
    json->set("read_time", fmt::format("{:.3f}ms", read_stage.read_time_ns.load() / NS_TO_MS_SCALE));
    return json;
}

Poco::JSON::Object::Ptr filterToJson(uint64_t input_rows, uint64_t selected_rows, uint64_t filtered_rows)
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("input_rows", input_rows);
    json->set("selected_rows", selected_rows);
    json->set("filtered_rows", filtered_rows);
    return json;
}
} // namespace

void ScanContext::setRegionNumOfCurrentInstance(uint64_t region_num)
{
    region_num_of_instance[current_instance_id] = region_num;
    // total_local_region_num may be updated later if some regions are not available in current instance.
    total_local_region_num = region_num;
}
void ScanContext::setStreamCost(
    uint64_t local_min_ns,
    uint64_t local_max_ns,
    uint64_t remote_min_ns,
    uint64_t remote_max_ns)
{
    local_min_stream_cost_ns = local_min_ns;
    local_max_stream_cost_ns = local_max_ns;
    remote_min_stream_cost_ns = remote_min_ns;
    remote_max_stream_cost_ns = remote_max_ns;
}

void ScanContext::serializeRegionNumOfInstance(tipb::TiFlashScanContext & proto) const
{
    for (const auto & [id, num] : region_num_of_instance)
    {
        auto * p = proto.add_regions_of_instance();
        p->set_instance_id(id);
        p->set_region_num(num);
    }
}

void ScanContext::deserializeRegionNumberOfInstance(const tipb::TiFlashScanContext & proto)
{
    for (const auto & t : proto.regions_of_instance())
    {
        region_num_of_instance[t.instance_id()] = t.region_num();
    }
}

void ScanContext::mergeRegionNumberOfInstance(const ScanContext & other)
{
    for (const auto & [id, num] : other.region_num_of_instance)
    {
        region_num_of_instance[id] += num;
    }
}

void ScanContext::mergeRegionNumberOfInstance(const tipb::TiFlashScanContext & other)
{
    for (const auto & t : other.regions_of_instance())
    {
        region_num_of_instance[t.instance_id()] += t.region_num();
    }
}

void ScanContext::mergeStreamCost(
    uint64_t local_min_ns,
    uint64_t local_max_ns,
    uint64_t remote_min_ns,
    uint64_t remote_max_ns)
{
    if (local_min_stream_cost_ns == 0 || local_min_ns < local_min_stream_cost_ns)
        local_min_stream_cost_ns = local_min_ns;
    if (local_max_ns > local_max_stream_cost_ns)
        local_max_stream_cost_ns = local_max_ns;
    if (remote_min_stream_cost_ns == 0 || remote_min_ns < remote_min_stream_cost_ns)
        remote_min_stream_cost_ns = remote_min_ns;
    if (remote_max_ns > remote_max_stream_cost_ns)
        remote_max_stream_cost_ns = remote_max_ns;
}

String ScanContext::toJson() const
{
    static constexpr double NS_TO_MS_SCALE = 1'000'000.0;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("dmfile_data_scanned_rows", dmfile_data_scanned_rows.load());
    json->set("dmfile_data_skipped_rows", dmfile_data_skipped_rows.load());
    json->set("dmfile_mvcc_scanned_rows", dmfile_mvcc_scanned_rows.load());
    json->set("dmfile_mvcc_skipped_rows", dmfile_mvcc_skipped_rows.load());
    json->set("dmfile_lm_filter_scanned_rows", dmfile_lm_filter_scanned_rows.load());
    json->set("dmfile_lm_filter_skipped_rows", dmfile_lm_filter_skipped_rows.load());
    json->set("dmfile_read_time", fmt::format("{:.3f}ms", total_dmfile_read_time_ns.load() / NS_TO_MS_SCALE));

    json->set(
        "rs_pack_filter_check_time",
        fmt::format("{:.3f}ms", total_rs_pack_filter_check_time_ns.load() / NS_TO_MS_SCALE));
    json->set("rs_pack_filter_none", rs_pack_filter_none.load());
    json->set("rs_pack_filter_some", rs_pack_filter_some.load());
    json->set("rs_pack_filter_all", rs_pack_filter_all.load());
    json->set("rs_pack_filter_all_null", rs_pack_filter_all_null.load());
    json->set("rs_dmfile_read_with_all", rs_dmfile_read_with_all.load());

    json->set("num_remote_region", total_remote_region_num.load());
    json->set("num_local_region", total_local_region_num.load());
    json->set("num_stale_read", num_stale_read.load());

    json->set("query_read_bytes", query_read_bytes.load());
    json->set("mvcc_read_bytes", mvcc_read_bytes.load());

    if (disagg_read_cache_hit_size.load() > 0 && disagg_read_cache_miss_size.load() > 0)
    {
        json->set("disagg_cache_hit_size", disagg_read_cache_hit_size.load());
        json->set("disagg_cache_miss_size", disagg_read_cache_miss_size.load());
    }

    json->set("num_segments", num_segments.load());
    json->set("num_read_tasks", num_read_tasks.load());
    json->set("num_columns", num_columns.load());

    json->set("delta_rows", delta_rows.load());
    json->set("delta_bytes", delta_bytes.load());

    // Note we must wrap the result of `magic_enum::enum_name` with `String`,
    // or Poco can not turn it into JSON correctly and crash
    json->set("read_mode", String(magic_enum::enum_name(read_mode)));

    json->set("mvcc_input_rows", mvcc_input_rows.load());
    json->set("mvcc_input_bytes", mvcc_input_bytes.load());
    json->set("mvcc_skip_rows", mvcc_input_rows.load() - mvcc_output_rows.load());
    json->set("late_materialization_skip_rows", late_materialization_skip_rows.load());

    json->set("learner_read_time", fmt::format("{:.3f}ms", learner_read_ns.load() / NS_TO_MS_SCALE));
    json->set("create_snapshot_time", fmt::format("{:.3f}ms", create_snapshot_time_ns.load() / NS_TO_MS_SCALE));
    json->set("build_stream_time", fmt::format("{:.3f}ms", build_inputstream_time_ns.load() / NS_TO_MS_SCALE));
    json->set("build_bitmap_time", fmt::format("{:.3f}ms", build_bitmap_time_ns.load() / NS_TO_MS_SCALE));

    json->set("local_min_stream_cost_ms", fmt::format("{:.3f}ms", local_min_stream_cost_ns / NS_TO_MS_SCALE));
    json->set("local_max_stream_cost_ms", fmt::format("{:.3f}ms", local_max_stream_cost_ns / NS_TO_MS_SCALE));
    json->set("remote_min_stream_cost_ms", fmt::format("{:.3f}ms", remote_min_stream_cost_ns / NS_TO_MS_SCALE));
    json->set("remote_max_stream_cost_ms", fmt::format("{:.3f}ms", remote_max_stream_cost_ns / NS_TO_MS_SCALE));

    auto to_json_object = [](const String & id, uint64_t num) {
        Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
        json->set("instance_id", id);
        json->set("region_num", num);
        return json;
    };
    auto to_json_array = [&to_json_object](const RegionNumOfInstance & region_num_of_instance) {
        Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
        for (const auto & [id, num] : region_num_of_instance)
        {
            arr->add(to_json_object(id, num));
        }
        return arr;
    };
    json->set("region_num_of_instance", to_json_array(region_num_of_instance));

    if (total_vector_idx_load_from_cache.load() //
            + total_vector_idx_load_from_disk.load() //
            + total_vector_idx_load_from_s3.load()
        > 0)
    {
        Poco::JSON::Object::Ptr vec_idx = new Poco::JSON::Object();
        vec_idx->set("tot_load", total_vector_idx_load_time_ms.load());
        vec_idx->set("load_s3", total_vector_idx_load_from_s3.load());
        vec_idx->set("load_disk", total_vector_idx_load_from_disk.load());
        vec_idx->set("load_cache", total_vector_idx_load_from_cache.load());
        vec_idx->set("tot_search", total_vector_idx_search_time_ms.load());
        vec_idx->set("read_vec", total_vector_idx_read_vec_time_ms.load());
        vec_idx->set("read_others", total_vector_idx_read_others_time_ms.load());
        json->set("vector_idx", vec_idx);
    }

    if (pushdown_executor)
    {
        json->set("pushdown", pushdown_executor->toJSONObject());
    }

    if (hasMultiStageLateMaterializationContext())
    {
        const auto & stats = getMultiStageLateMaterializationRuntimeStats();
        Poco::JSON::Object::Ptr mslm = new Poco::JSON::Object();
        mslm->set("streams", stats.finished_streams.load());
        mslm->set("late_mode_blocks", stats.late_mode_blocks.load());
        mslm->set("direct_mode_blocks", stats.direct_mode_blocks.load());
        mslm->set("pushed_filter_read", readStageToJson(mslm_pushed_filter_read));
        mslm->set("candidate_read", readStageToJson(mslm_candidate_read));
        mslm->set("final_rest_read", readStageToJson(mslm_final_rest_read));
        mslm->set(
            "pushed_filter",
            filterToJson(
                stats.pushed_filter_input_rows.load(),
                stats.pushed_filter_selected_rows.load(),
                stats.pushed_filter_filtered_rows.load()));
        mslm->set(
            "residual_filter",
            filterToJson(
                stats.residual_filter_input_rows.load(),
                stats.residual_filter_selected_rows.load(),
                stats.residual_filter_filtered_rows.load()));
        if (stats.topn_enabled.load())
        {
            Poco::JSON::Object::Ptr running_topn = new Poco::JSON::Object();
            running_topn->set("input_rows", stats.running_topn_input_rows.load());
            running_topn->set("selected_rows", stats.running_topn_selected_rows.load());
            running_topn->set("bypass_rows", stats.running_topn_bypass_rows.load());
            running_topn->set("filtered_rows", stats.running_topn_filtered_rows.load());
            running_topn->set("heap_size_sum", stats.topn_heap_size_sum.load());
            running_topn->set("adaptive_warmup_rows", stats.topn_adaptive_warmup_rows.load());
            running_topn->set("adaptive_post_warmup_input_rows", stats.topn_adaptive_post_warmup_input_rows.load());
            running_topn->set(
                "adaptive_post_warmup_candidate_rows",
                stats.topn_adaptive_post_warmup_candidate_rows.load());
            running_topn->set("adaptive_disabled_streams", stats.topn_adaptive_disabled_streams.load());
            mslm->set("running_topn", running_topn);
        }
        mslm->set("final_rest_input_rows", stats.final_rest_input_rows.load());
        json->set("multi_stage_late_materialization", mslm);
    }

    std::stringstream buf;
    json->stringify(buf);
    return buf.str();
}

String getHostName(const LoggerPtr & log)
{
    char hostname[1024];
    if (::gethostname(hostname, sizeof(hostname)) != 0)
    {
        LOG_ERROR(log, "gethostname failed: {}", errno);
        return {};
    }
    return hostname;
}

bool isLocalAddress(const String & address)
{
    static const std::vector<String> local_list{// ivp4
                                                "0.0.0.0",
                                                "127.",
                                                "localhost",
                                                // ipv6
                                                "0:0:0:0:0:0:0",
                                                "[0:0:0:0:0:0:0",
                                                ":",
                                                "[:"};
    for (const auto & local_prefix : local_list)
    {
        if (address.starts_with(local_prefix))
        {
            return true;
        }
    }
    return false;
}

String getPort(const String & address)
{
    auto pos = address.find_last_of(':');
    if (pos == std::string::npos)
    {
        return {};
    }
    return address.substr(pos + 1);
}

String getCurrentInstanceId(const String & flash_server_addr, const LoggerPtr & log)
{
    if (!isLocalAddress(flash_server_addr))
    {
        return flash_server_addr;
    }

    auto hostname = getHostName(log);
    if (hostname.empty())
    {
        return Poco::UUIDGenerator().createRandom().toString();
    }

    auto port = getPort(flash_server_addr);
    if (!port.empty())
    {
        return hostname + ":" + port;
    }
    else
    {
        auto uuid = Poco::UUIDGenerator().createRandom().toString();
        // hostname + uuid may too long, so cut the uuid.
        return hostname + "-" + uuid.substr(0, std::min(8, uuid.size()));
    }
}

void ScanContext::initCurrentInstanceId(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log)
{
    auto flash_server_addr = config.getString("flash.service_addr", "0.0.0.0:3930");
    current_instance_id = getCurrentInstanceId(flash_server_addr, log);
    LOG_INFO(log, "flash_server_addr={}, current_instance_id={}", flash_server_addr, current_instance_id);
}

std::optional<LACBytesCollector> ScanContext::newLACBytesCollector(ReadTag read_tag)
{
    if (resource_group_name.empty())
        return std::nullopt;
    if (!isQueryReadTag(read_tag))
        return std::nullopt;
    return LACBytesCollector(resource_group_name);
}

void ScanContext::addUserReadBytes(
    size_t bytes,
    ReadTag read_tag,
    std::optional<LACBytesCollector> & lac_bytes_collector)
{
    if (!isQueryReadTag(read_tag) && read_tag != ReadTag::MVCC)
        return;
    if (read_tag == ReadTag::MVCC)
    {
        mvcc_read_bytes += bytes;
        if (mvcc_read_bytes_counter)
            mvcc_read_bytes_counter->Increment(bytes);
    }
    else
    {
        query_read_bytes += bytes;
        if (auto * read_stage = getMutableMSLMReadStage(read_tag); read_stage != nullptr)
            read_stage->read_bytes += bytes;
        if (query_read_bytes_counter)
            query_read_bytes_counter->Increment(bytes);
        if (lac_bytes_collector)
            lac_bytes_collector->collect(bytes);
    }
}

bool ScanContext::hasMultiStageLateMaterializationContext() const
{
    return multi_stage_late_materialization_enabled.load(std::memory_order_relaxed)
        || multi_stage_late_materialization_runtime_stats != nullptr;
}

const MultiStageLateMaterializationRuntimeStats & ScanContext::getMultiStageLateMaterializationRuntimeStats() const
{
    if (multi_stage_late_materialization_runtime_stats)
        return *multi_stage_late_materialization_runtime_stats;
    return merged_multi_stage_late_materialization_runtime_stats;
}

MultiStageLateMaterializationRuntimeStats & ScanContext::getMergedMultiStageLateMaterializationRuntimeStats()
{
    multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
    return merged_multi_stage_late_materialization_runtime_stats;
}

MSLMReadStageScanContext * ScanContext::getMutableMSLMReadStage(ReadTag read_tag)
{
    switch (read_tag)
    {
    case ReadTag::MSLMPushedFilter:
        multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
        return &mslm_pushed_filter_read;
    case ReadTag::MSLMCandidate:
        multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
        return &mslm_candidate_read;
    case ReadTag::MSLMFinalRest:
        multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
        return &mslm_final_rest_read;
    default:
        return nullptr;
    }
}

const MSLMReadStageScanContext * ScanContext::getMSLMReadStage(ReadTag read_tag) const
{
    switch (read_tag)
    {
    case ReadTag::MSLMPushedFilter:
        return &mslm_pushed_filter_read;
    case ReadTag::MSLMCandidate:
        return &mslm_candidate_read;
    case ReadTag::MSLMFinalRest:
        return &mslm_final_rest_read;
    default:
        return nullptr;
    }
}

void ScanContext::addDMFileReadTime(uint64_t ns, ReadTag read_tag)
{
    total_dmfile_read_time_ns += ns;
    if (auto * read_stage = getMutableMSLMReadStage(read_tag); read_stage != nullptr)
        read_stage->read_time_ns += ns;
}

void ScanContext::addDMFileScannedRows(uint64_t rows, ReadTag read_tag)
{
    switch (read_tag)
    {
    case ReadTag::Query:
        dmfile_data_scanned_rows += rows;
        break;
    case ReadTag::MVCC:
        dmfile_mvcc_scanned_rows += rows;
        break;
    case ReadTag::LMFilter:
        dmfile_lm_filter_scanned_rows += rows;
        break;
    case ReadTag::MSLMPushedFilter:
    case ReadTag::MSLMCandidate:
    case ReadTag::MSLMFinalRest:
        getMutableMSLMReadStage(read_tag)->dmfile_scanned_rows += rows;
        break;
    default:
        break;
    }
}

void ScanContext::addDMFileSkippedRows(uint64_t rows, ReadTag read_tag)
{
    switch (read_tag)
    {
    case ReadTag::Query:
        dmfile_data_skipped_rows += rows;
        break;
    case ReadTag::MVCC:
        dmfile_mvcc_skipped_rows += rows;
        break;
    case ReadTag::LMFilter:
        dmfile_lm_filter_skipped_rows += rows;
        break;
    case ReadTag::MSLMPushedFilter:
    case ReadTag::MSLMCandidate:
    case ReadTag::MSLMFinalRest:
        getMutableMSLMReadStage(read_tag)->dmfile_skipped_rows += rows;
        break;
    default:
        break;
    }
}

void ScanContext::setMultiStageLateMaterializationRuntimeStats(
    const MultiStageLateMaterializationRuntimeStatsPtr & stats)
{
    multi_stage_late_materialization_runtime_stats = stats;
    if (stats != nullptr)
        multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
}

void ScanContext::serializeMultiStageLateMaterialization(tipb::TiFlashScanContext & proto) const
{
    if (!hasMultiStageLateMaterializationContext())
        return;

    const auto & stats = getMultiStageLateMaterializationRuntimeStats();
    auto * mslm = proto.mutable_multi_stage_late_materialization();
    mslm->set_streams(stats.finished_streams.load(std::memory_order_relaxed));
    mslm->set_late_mode_blocks(stats.late_mode_blocks.load(std::memory_order_relaxed));
    mslm->set_direct_mode_blocks(stats.direct_mode_blocks.load(std::memory_order_relaxed));
    serializeReadStage(mslm_pushed_filter_read, mslm->mutable_pushed_filter_read());
    serializeReadStage(mslm_candidate_read, mslm->mutable_candidate_read());
    serializeReadStage(mslm_final_rest_read, mslm->mutable_final_rest_read());
    serializeFilter(
        stats.pushed_filter_input_rows.load(std::memory_order_relaxed),
        stats.pushed_filter_selected_rows.load(std::memory_order_relaxed),
        stats.pushed_filter_filtered_rows.load(std::memory_order_relaxed),
        mslm->mutable_pushed_filter());
    serializeFilter(
        stats.residual_filter_input_rows.load(std::memory_order_relaxed),
        stats.residual_filter_selected_rows.load(std::memory_order_relaxed),
        stats.residual_filter_filtered_rows.load(std::memory_order_relaxed),
        mslm->mutable_residual_filter());
    if (stats.topn_enabled.load(std::memory_order_relaxed))
    {
        auto * topn = mslm->mutable_running_topn();
        topn->set_input_rows(stats.running_topn_input_rows.load(std::memory_order_relaxed));
        topn->set_selected_rows(stats.running_topn_selected_rows.load(std::memory_order_relaxed));
        topn->set_bypass_rows(stats.running_topn_bypass_rows.load(std::memory_order_relaxed));
        topn->set_filtered_rows(stats.running_topn_filtered_rows.load(std::memory_order_relaxed));
        topn->set_heap_size_sum(stats.topn_heap_size_sum.load(std::memory_order_relaxed));
        topn->set_adaptive_warmup_rows(stats.topn_adaptive_warmup_rows.load(std::memory_order_relaxed));
        topn->set_adaptive_post_warmup_input_rows(
            stats.topn_adaptive_post_warmup_input_rows.load(std::memory_order_relaxed));
        topn->set_adaptive_post_warmup_candidate_rows(
            stats.topn_adaptive_post_warmup_candidate_rows.load(std::memory_order_relaxed));
        topn->set_adaptive_disabled_streams(stats.topn_adaptive_disabled_streams.load(std::memory_order_relaxed));
    }
    mslm->set_final_rest_input_rows(stats.final_rest_input_rows.load(std::memory_order_relaxed));
}

void ScanContext::deserializeMultiStageLateMaterialization(const tipb::TiFlashScanContext & proto)
{
    if (!proto.has_multi_stage_late_materialization())
        return;
    mergeMultiStageLateMaterialization(proto);
}

void ScanContext::mergeMultiStageLateMaterialization(const ScanContext & other)
{
    if (!other.hasMultiStageLateMaterializationContext())
        return;

    multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);
    mslm_pushed_filter_read.merge(other.mslm_pushed_filter_read);
    mslm_candidate_read.merge(other.mslm_candidate_read);
    mslm_final_rest_read.merge(other.mslm_final_rest_read);
    merged_multi_stage_late_materialization_runtime_stats.merge(other.getMultiStageLateMaterializationRuntimeStats());
}

void ScanContext::mergeMultiStageLateMaterialization(const tipb::TiFlashScanContext & other)
{
    if (!other.has_multi_stage_late_materialization())
        return;

    const auto & mslm = other.multi_stage_late_materialization();
    multi_stage_late_materialization_enabled.store(true, std::memory_order_relaxed);

    if (mslm.has_pushed_filter_read())
        mergeReadStage(mslm_pushed_filter_read, mslm.pushed_filter_read());
    if (mslm.has_candidate_read())
        mergeReadStage(mslm_candidate_read, mslm.candidate_read());
    if (mslm.has_final_rest_read())
        mergeReadStage(mslm_final_rest_read, mslm.final_rest_read());

    auto & stats = getMergedMultiStageLateMaterializationRuntimeStats();
    stats.finished_streams += mslm.streams();
    stats.late_mode_blocks += mslm.late_mode_blocks();
    stats.direct_mode_blocks += mslm.direct_mode_blocks();
    if (mslm.has_pushed_filter())
    {
        const auto & pushed_filter = mslm.pushed_filter();
        stats.pushed_filter_input_rows += pushed_filter.input_rows();
        stats.pushed_filter_selected_rows += pushed_filter.selected_rows();
        stats.pushed_filter_filtered_rows += pushed_filter.filtered_rows();
        stats.stage0_output_rows += pushed_filter.selected_rows();
    }
    if (mslm.has_residual_filter())
    {
        const auto & residual_filter = mslm.residual_filter();
        stats.residual_filter_input_rows += residual_filter.input_rows();
        stats.residual_filter_selected_rows += residual_filter.selected_rows();
        stats.residual_filter_filtered_rows += residual_filter.filtered_rows();
        stats.stage1_output_rows += residual_filter.selected_rows();
    }
    stats.final_rest_input_rows += mslm.final_rest_input_rows();
    stats.topn_candidate_rows += mslm.final_rest_input_rows();
    if (mslm.has_running_topn())
    {
        const auto & running_topn = mslm.running_topn();
        stats.topn_enabled.store(true, std::memory_order_relaxed);
        stats.running_topn_input_rows += running_topn.input_rows();
        stats.running_topn_selected_rows += running_topn.selected_rows();
        stats.running_topn_bypass_rows += running_topn.bypass_rows();
        stats.running_topn_filtered_rows += running_topn.filtered_rows();
        stats.topn_heap_size_sum += running_topn.heap_size_sum();
        stats.topn_adaptive_warmup_rows += running_topn.adaptive_warmup_rows();
        stats.topn_adaptive_post_warmup_input_rows += running_topn.adaptive_post_warmup_input_rows();
        stats.topn_adaptive_post_warmup_candidate_rows += running_topn.adaptive_post_warmup_candidate_rows();
        stats.topn_adaptive_disabled_streams += running_topn.adaptive_disabled_streams();
    }
}

} // namespace DB::DM

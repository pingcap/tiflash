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
#include <Storages/DeltaMerge/ScanContext.h>

#include <magic_enum.hpp>

namespace DB::DM
{
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
        fmt::format("{:.3f}ms", total_dmfile_rough_set_index_check_time_ns.load() / NS_TO_MS_SCALE));
    json->set("rs_pack_filter_none", rs_pack_filter_none.load());
    json->set("rs_pack_filter_some", rs_pack_filter_some.load());
    json->set("rs_pack_filter_all", rs_pack_filter_all.load());

    json->set("num_remote_region", total_remote_region_num.load());
    json->set("num_local_region", total_local_region_num.load());
    json->set("num_stale_read", num_stale_read.load());

    json->set("read_bytes", user_read_bytes.load());

    json->set("disagg_cache_hit_size", disagg_read_cache_hit_size.load());
    json->set("disagg_cache_miss_size", disagg_read_cache_miss_size.load());

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
} // namespace DB::DM

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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop
#include <Storages/DeltaMerge/ScanContext.h>

#include <magic_enum.hpp>

namespace DB::DM
{
String ScanContext::toJson() const
{
    static constexpr double NS_TO_MS_SCALE = 1'000'000.0;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("dmfile_scan_rows", total_dmfile_scanned_rows.load());
    json->set("dmfile_skip_rows", total_dmfile_skipped_rows.load());
    json->set("dmfile_read_time", fmt::format("{:.3f}ms", total_dmfile_read_time_ns.load() / NS_TO_MS_SCALE));

    json->set("num_remote_region", total_remote_region_num.load());
    json->set("num_local_region", total_remote_region_num.load());
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

    std::stringstream buf;
    json->stringify(buf);
    return buf.str();
}
} // namespace DB::DM

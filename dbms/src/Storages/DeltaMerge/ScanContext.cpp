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

namespace DB::DM
{
String ScanContext::toJson() const
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("dmfile_scan_rows", total_dmfile_scanned_rows.load());
    json->set("dmfile_skip_rows", total_dmfile_skipped_rows.load());
    json->set("dmfile_read_time", fmt::format("{}ms", total_dmfile_read_time_ms.load()));
    json->set("create_snapshot_time", fmt::format("{}ms", total_create_snapshot_time_ms.load()));
    json->set("create_inputstream_time", fmt::format("{}ms", total_create_inputstream_time_ms.load()));

    json->set("num_segments", num_segments.load());
    json->set("num_read_tasks", num_read_tasks.load());
    json->set("num_columns", num_columns.load());

    json->set("delta_rows", delta_rows.load());
    json->set("delta_bytes", delta_bytes.load());

    json->set("mvcc_input_rows", mvcc_input_rows.load());
    json->set("mvcc_input_bytes", mvcc_input_bytes.load());
    json->set("mvcc_output_rows", mvcc_output_rows.load());

    std::stringstream buf;
    json->stringify(buf);
    return buf.str();
}
} // namespace DB::DM

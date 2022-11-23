// Copyright 2022 PingCAP, Ltd.
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

#pragma once

#include <common/types.h>
#include <fmt/format.h>
#include <sys/types.h>
#include <tipb/executor.pb.h>

#include <atomic>

namespace DB::DM
{
class ScanContext
{
public:
    // sum of scanned packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_scanned_packs_in_dmfile{0};

    // sum of skipped packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_skipped_packs_in_dmfile{0};

    // sum of scanned rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_scanned_rows_in_dmfile{0};

    // sum of skipped rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_skipped_rows_in_dmfile{0};

    std::atomic<uint64_t> total_rough_set_index_load_time_in_ns{0};
    std::atomic<uint64_t> total_dmfile_read_time_in_ns{0};
    std::atomic<uint64_t> total_create_snapshot_time_in_ns{0};

    ScanContext() = default;

    explicit ScanContext(const tipb::TiFlashScanContext & tiflash_scan_context_pb)
    {
        total_scanned_packs_in_dmfile = tiflash_scan_context_pb.total_scanned_packs_in_dmfile();
        total_skipped_packs_in_dmfile = tiflash_scan_context_pb.total_skipped_packs_in_dmfile();
        total_scanned_rows_in_dmfile = tiflash_scan_context_pb.total_scanned_rows_in_dmfile();
        total_skipped_rows_in_dmfile = tiflash_scan_context_pb.total_skipped_rows_in_dmfile();
        total_rough_set_index_load_time_in_ns = tiflash_scan_context_pb.total_rough_set_index_load_time_in_ns();
        total_dmfile_read_time_in_ns = tiflash_scan_context_pb.total_dmfile_read_time_in_ns();
        total_create_snapshot_time_in_ns = tiflash_scan_context_pb.total_create_snapshot_time_in_ns();
    }


    void merge(const ScanContext * other)
    {
        total_scanned_packs_in_dmfile += other->total_scanned_packs_in_dmfile;
        total_skipped_packs_in_dmfile += other->total_skipped_packs_in_dmfile;
        total_scanned_rows_in_dmfile += other->total_scanned_rows_in_dmfile;
        total_skipped_rows_in_dmfile += other->total_skipped_rows_in_dmfile;
        total_rough_set_index_load_time_in_ns += other->total_rough_set_index_load_time_in_ns;
        total_dmfile_read_time_in_ns += other->total_dmfile_read_time_in_ns;
        total_create_snapshot_time_in_ns += other->total_create_snapshot_time_in_ns;
    }
};

using ScanContextPtr = std::shared_ptr<ScanContext>;

} // namespace DB::DM
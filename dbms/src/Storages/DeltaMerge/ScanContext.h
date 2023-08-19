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

#pragma once

#include <common/types.h>
#include <fmt/format.h>
#include <sys/types.h>
#include <tipb/executor.pb.h>

#include <atomic>

namespace DB::DM
{
/// ScanContext is used to record statistical information in table scan for current query.
/// For each table scan(one executor id), there is only one ScanContext.
/// ScanContext helps to collect the statistical information of the table scan to show in `EXPLAIN ANALYZE`.
class ScanContext
{
public:
    /// sum of scanned packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_scanned_packs{0};

    /// sum of skipped packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_skipped_packs{0};

    /// sum of scanned rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_scanned_rows{0};

    /// sum of skipped rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_skipped_rows{0};

    std::atomic<uint64_t> total_dmfile_rough_set_index_load_time_ms{0};
    std::atomic<uint64_t> total_dmfile_read_time_ms{0};
    std::atomic<uint64_t> total_create_snapshot_time_ms{0};

    ScanContext() = default;

    void deserialize(const tipb::TiFlashScanContext & tiflash_scan_context_pb)
    {
        total_dmfile_scanned_packs = tiflash_scan_context_pb.total_dmfile_scanned_packs();
        total_dmfile_skipped_packs = tiflash_scan_context_pb.total_dmfile_skipped_packs();
        total_dmfile_scanned_rows = tiflash_scan_context_pb.total_dmfile_scanned_rows();
        total_dmfile_skipped_rows = tiflash_scan_context_pb.total_dmfile_skipped_rows();
        total_dmfile_rough_set_index_load_time_ms = tiflash_scan_context_pb.total_dmfile_rough_set_index_load_time_ms();
        total_dmfile_read_time_ms = tiflash_scan_context_pb.total_dmfile_read_time_ms();
        total_create_snapshot_time_ms = tiflash_scan_context_pb.total_create_snapshot_time_ms();
    }

    tipb::TiFlashScanContext serialize()
    {
        tipb::TiFlashScanContext tiflash_scan_context_pb{};
        tiflash_scan_context_pb.set_total_dmfile_scanned_packs(total_dmfile_scanned_packs);
        tiflash_scan_context_pb.set_total_dmfile_skipped_packs(total_dmfile_skipped_packs);
        tiflash_scan_context_pb.set_total_dmfile_scanned_rows(total_dmfile_scanned_rows);
        tiflash_scan_context_pb.set_total_dmfile_skipped_rows(total_dmfile_skipped_rows);
        tiflash_scan_context_pb.set_total_dmfile_rough_set_index_load_time_ms(total_dmfile_rough_set_index_load_time_ms);
        tiflash_scan_context_pb.set_total_dmfile_read_time_ms(total_dmfile_read_time_ms);
        tiflash_scan_context_pb.set_total_create_snapshot_time_ms(total_create_snapshot_time_ms);
        return tiflash_scan_context_pb;
    }

    void merge(const ScanContext & other)
    {
        total_dmfile_scanned_packs += other.total_dmfile_scanned_packs;
        total_dmfile_skipped_packs += other.total_dmfile_skipped_packs;
        total_dmfile_scanned_rows += other.total_dmfile_scanned_rows;
        total_dmfile_skipped_rows += other.total_dmfile_skipped_rows;
        total_dmfile_rough_set_index_load_time_ms += other.total_dmfile_rough_set_index_load_time_ms;
        total_dmfile_read_time_ms += other.total_dmfile_read_time_ms;
        total_create_snapshot_time_ms += other.total_create_snapshot_time_ms;
    }
};

using ScanContextPtr = std::shared_ptr<ScanContext>;

} // namespace DB::DM
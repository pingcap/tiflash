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

#include <fmt/format.h>

#include <atomic>

#include "common/types.h"
#include "tipb/executor.pb.h"

namespace DB::DM
{

class FullTableScanContext
{
public:
    std::atomic<uint64_t> scan_packs_count{0}; // number of scan packs
    std::atomic<uint64_t> skip_packs_count{0}; // number of skip packs

    std::atomic<uint64_t> scan_rows_count{0}; // number of scan rows
    std::atomic<uint64_t> skip_rows_count{0}; // number of skip rows

    FullTableScanContext() = default;

    explicit FullTableScanContext(const tipb::FullTableScanContext full_table_scan_context_pb)
    {
        scan_packs_count = full_table_scan_context_pb.scan_packs_count();
        skip_packs_count = full_table_scan_context_pb.skip_packs_count();
        scan_rows_count = full_table_scan_context_pb.scan_rows_count();
        skip_rows_count = full_table_scan_context_pb.skip_rows_count();
    }


    void merge(const FullTableScanContext * other)
    {
        std::cout << " FullTableScanContext info " << toDebugString() << " and merge with " << other->toDebugString() << std::endl;

        scan_packs_count += other->scan_packs_count;
        skip_packs_count += other->skip_packs_count;
        scan_rows_count += other->scan_rows_count;
        skip_rows_count += other->skip_rows_count;
    }

    String toDebugString() const
    {
        return fmt::format("scan_packs_count: {}, skip_packs_count: {}, scan_rows_count: {}, skip_rows_count: {}",
                           scan_packs_count.load(),
                           skip_packs_count.load(),
                           scan_rows_count.load(),
                           skip_rows_count.load());
    }
};

} // namespace DB::DM
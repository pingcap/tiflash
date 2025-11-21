// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf_fwd.h>
#include <common/types.h>

namespace DB::DM
{

struct FullTextIndexPerf
{
    // NoIndex=Brute Search
    uint32_t n_from_inmemory_noindex = 0;
    uint32_t n_from_tiny_index = 0;
    uint32_t n_from_tiny_noindex = 0;
    uint32_t n_from_dmf_index = 0;
    uint32_t n_from_dmf_noindex = 0;
    uint64_t rows_from_inmemory_noindex = 0;
    uint64_t rows_from_tiny_index = 0;
    uint64_t rows_from_tiny_noindex = 0;
    uint64_t rows_from_dmf_index = 0;
    uint64_t rows_from_dmf_noindex = 0;

    // ============================================================
    // Below: During indexed search
    uint64_t idx_load_total_ms = 0; // this/idx_load_*=avg_load_ms
    uint32_t idx_load_from_cache = 0;
    uint32_t idx_load_from_column_file = 0; // contains ColumnFile load time
    uint32_t idx_load_from_stable_s3 = 0;
    uint32_t idx_load_from_stable_disk = 0;
    uint32_t idx_search_n = 0;
    uint64_t idx_search_total_ms = 0; // this/idx_search_n=avg_search_ms
    uint64_t idx_dm_search_rows = 0;
    uint64_t idx_dm_total_read_fts_ms = 0; // this/dm_search_rows=per_row_read_ms
    uint64_t idx_dm_total_read_others_ms = 0;
    uint64_t idx_tiny_search_rows = 0;
    uint64_t idx_tiny_total_read_fts_ms = 0; // this/tiny_search_rows=per_row_read_ms
    uint64_t idx_tiny_total_read_others_ms = 0;
    // ============================================================

    // ============================================================
    // Below: During brute search
    uint64_t brute_total_read_ms = 0; // FTS and other columns are not separated because they are read together
    uint64_t brute_total_search_ms = 0;
    // ============================================================

    static FullTextIndexPerfPtr create() { return std::make_shared<FullTextIndexPerf>(); }
};

} // namespace DB::DM
#endif

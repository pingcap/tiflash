// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Index/VectorIndex/Perf_fwd.h>
#include <common/types.h>

#include <memory>

namespace DB::DM
{

/// Unlike FileCache, this is supposed to be wrapped inside a shared_ptr because an input stream could be
/// accessed in different threads (although there is no concurrent access) throughout its lifetime. So
/// thread local is no longer useful.
struct VectorIndexPerf
{
    uint32_t n_from_cf_index = 0;
    uint32_t n_from_cf_noindex = 0;
    uint32_t n_from_dmf_index = 0;
    uint32_t n_from_dmf_noindex = 0;

    // ============================================================
    // Below: During search
    uint32_t n_searches = 0;
    uint32_t total_search_ms = 0;
    uint64_t visited_nodes = 0;
    uint64_t discarded_nodes = 0; // Rows filtered out by MVCC
    uint64_t returned_nodes = 0;
    uint32_t n_dm_searches = 0; // For calculating avg below
    uint32_t dm_packs_in_file = 0;
    uint32_t dm_packs_before_search = 0;
    uint32_t dm_packs_after_search = 0;
    // ============================================================

    // ============================================================
    // Below: During loading index
    uint32_t total_load_ms = 0;
    uint32_t load_from_cache = 0;
    uint32_t load_from_column_file = 0; // contains ColumnFile load time
    uint32_t load_from_stable_s3 = 0;
    uint32_t load_from_stable_disk = 0;
    // ============================================================

    // ============================================================
    // Below: During reading column data
    uint32_t n_dm_reads = 0; // For calculating avg below
    uint32_t total_dm_read_vec_ms = 0;
    uint32_t total_dm_read_others_ms = 0;
    uint32_t n_cf_reads = 0; // For calculating avg below
    uint32_t total_cf_read_vec_ms = 0;
    uint32_t total_cf_read_others_ms = 0;
    // ============================================================

    static VectorIndexPerfPtr create() { return std::make_shared<VectorIndexPerf>(); }
};

} // namespace DB::DM

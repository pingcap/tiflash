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

#include <Columns/IColumn.h>
#include <Interpreters/Settings.h>

namespace DB
{

/// The max_block_size upper bound of hash join.
#define HASH_JOIN_MAX_BLOCK_SIZE_UPPER_BOUND (65536 * 2)

const IColumn::Offsets BASE_OFFSETS = [] {
    IColumn::Offsets offsets(HASH_JOIN_MAX_BLOCK_SIZE_UPPER_BOUND);
    std::iota(offsets.begin(), offsets.end(), 0ULL);
    return offsets;
}();

struct HashJoinSettings
{
    explicit HashJoinSettings(const Settings & settings)
        : max_block_size(std::min(
            HASH_JOIN_MAX_BLOCK_SIZE_UPPER_BOUND,
            std::min(settings.max_block_size, settings.join_v2_max_block_size)))
        , probe_enable_prefetch_threshold(settings.join_v2_probe_enable_prefetch_threshold)
        , probe_prefetch_step(settings.join_v2_probe_prefetch_step)
        , probe_insert_batch_size(settings.join_v2_probe_insert_batch_size)
        , enable_tagged_pointer(settings.join_v2_enable_tagged_pointer)
    {}
    const size_t max_block_size;
    const size_t probe_enable_prefetch_threshold;
    const size_t probe_prefetch_step;
    const size_t probe_insert_batch_size;
    const bool enable_tagged_pointer;
};

} // namespace DB

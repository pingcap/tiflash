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

#include <cstdint>
#include <common/types.h>

namespace DB
{

namespace DM
{

class PerfContext {
public:
    uint64_t scan_packs_count;
    uint64_t scan_rows_count;
    uint64_t skip_packs_count;
    uint64_t skip_rows_count;

    uint64_t wait_index_cost_ns;

    // 扫到的列？-- 不如加到 log 中  
    void reset();  // reset all performance counters to zero

    String toDebugString() const;
};

PerfContext* get_perf_context();

} // namespace DM

} // namespace DB
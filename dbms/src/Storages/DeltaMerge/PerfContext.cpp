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
#include <Storages/DeltaMerge/PerfContext.h>
#include <string>

namespace DB 
{

namespace DM 
{

thread_local PerfContext perf_context;
//PerfContext perf_context;

PerfContext* get_perf_context() { return &perf_context; }

void PerfContext::reset() {
    scan_packs_count = 0;
    scan_rows_count = 0;
    skip_packs_count = 0;
    skip_rows_count = 0;
    wait_index_cost_ns = 0;
}

String PerfContext::toDebugString() const {
    String res = " scan_packs_count = " + std::to_string(scan_packs_count) + " scan_rows_count = " + std::to_string(scan_rows_count)
            + " skip_packs_count = " + std::to_string(skip_packs_count) + " skip_rows_count = " + std::to_string(skip_rows_count)
            + " wait_index_cost_ns = " + std::to_string(wait_index_cost_ns);
    return res; 
}

}
}

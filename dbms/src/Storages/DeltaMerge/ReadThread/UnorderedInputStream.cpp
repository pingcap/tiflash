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

#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>

namespace DB::DM
{

void UnorderedInputStream::prepareRuntimeFilter()
{
    if (runtime_filter_list.empty())
    {
        return;
    }
    // wait for runtime filter ready
    Stopwatch sw;
    std::vector<RuntimeFilterPtr> ready_rf_list;
    for (const RuntimeFilterPtr & rf : runtime_filter_list)
    {
        bool is_ready = rf->await(max_wait_time_ms - sw.elapsedMilliseconds());
        if (is_ready)
        {
            ready_rf_list.push_back(rf);
        }
    }
    // append ready rfs into push down filter
    pushDownReadyRFList(ready_rf_list);
}

void UnorderedInputStream::pushDownReadyRFList(const std::vector<RuntimeFilterPtr> & ready_rf_list)
{
    for (const RuntimeFilterPtr & rf : ready_rf_list)
    {
        auto rs_operator = rf->parseToRSOperator();
        task_pool->appendRSOperator(rs_operator);
    }
}
} // namespace DB::DM

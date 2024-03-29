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

#include <Common/Logger.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/Page/PageDefinesBase.h>
#include <common/logger_useful.h>

#include <atomic>

namespace DB::DM
{

void GlobalPageIdAllocator::raiseTargetByLowerBound(std::atomic<PageIdU64> & target, PageIdU64 lower_bound)
{
    PageIdU64 old_value = target.load();
    while (true)
    {
        // already satisfied the lower_bound, done.
        if (old_value >= lower_bound)
            break;
        SYNC_FOR("before_GlobalPageIdAllocator::raiseLowerBoundCAS_1");
        // try raise to the lower_bound
        if (target.compare_exchange_strong(old_value, lower_bound))
        {
            // raise success, done.
            break;
        }
        // else the `old_value` is updated, try again
    }
}

} // namespace DB::DM

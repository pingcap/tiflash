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

#include <Storages/KVStore/Types.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/PageStorage_fwd.h>

#include <atomic>

namespace DB::DM
{

class GlobalPageIdAllocator : private boost::noncopyable
{
public:
    GlobalPageIdAllocator() = default;

    void raiseDataPageIdLowerBound(PageIdU64 lower_bound) { raiseTargetByLowerBound(max_data_page_id, lower_bound); }
    void raiseLogPageIdLowerBound(PageIdU64 lower_bound) { raiseTargetByLowerBound(max_log_page_id, lower_bound); }
    void raiseMetaPageIdLowerBound(PageIdU64 lower_bound) { raiseTargetByLowerBound(max_meta_page_id, lower_bound); }

    PageIdU64 newDataPageIdForDTFile() { return ++max_data_page_id; }
    PageIdU64 newLogPageId() { return ++max_log_page_id; }
    PageIdU64 newMetaPageId() { return ++max_meta_page_id; }

    std::tuple<PageIdU64, PageIdU64, PageIdU64> getCurrentIds() const
    {
        return std::make_tuple(max_log_page_id.load(), max_data_page_id.load(), max_meta_page_id.load());
    }

private:
    static void raiseTargetByLowerBound(std::atomic<PageIdU64> & target, PageIdU64 lower_bound);

private:
    std::atomic<PageIdU64> max_log_page_id = 0;
    std::atomic<PageIdU64> max_data_page_id = 0;
    // The meta_page_id == 1 is reserved for first segment in each physical table
    std::atomic<PageIdU64> max_meta_page_id = 1;
};

} // namespace DB::DM

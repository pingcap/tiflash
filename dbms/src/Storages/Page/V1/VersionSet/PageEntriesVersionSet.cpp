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

#include <Storages/Page/V1/VersionSet/PageEntriesVersionSet.h>

namespace DB::PS::V1
{
std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> PageEntriesVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_write_mutex);

    // apply edit on base
    PageEntries * v = nullptr;
    {
        PageEntriesBuilder builder(current);
        builder.gcApply(edit);
        v = builder.build();
    }

    this->appendVersion(v, lock);

    return listAllLiveFiles(lock);
}

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>>
PageEntriesVersionSet::listAllLiveFiles(const std::unique_lock<std::shared_mutex> & lock) const
{
    (void)lock;
    std::set<PageFileIdAndLevel> live_files;
    std::set<PageId> live_normal_pages;
    for (PageEntries * v = placeholder_node.next; v != &placeholder_node; v = v->next)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            live_normal_pages.insert(it->first);
            live_files.insert(it->second.fileIdLevel());
        }
    }
    return {live_files, live_normal_pages};
}


} // namespace DB::PS::V1

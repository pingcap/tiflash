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

#pragma once
#ifndef NDEBUG

#include <Storages/Page/Page.h>

#include <vector>

namespace DB::PS::V2::tests
{
class MockEntries
{
private:
    std::map<PageId, PageEntry> entries;

public:
    void put(PageId page_id, PageEntry entry) { entries[page_id] = entry; }

    std::set<PageId> validNormalPageIds() const
    {
        std::set<PageId> ids;
        for (auto it = entries.begin(); it != entries.end(); ++it)
            ids.insert(it->first);
        return ids;
    }

    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const
    {
        if (auto it = entries.find(page_id); it != entries.end())
            return it->second;
        else
            return std::nullopt;
    }
};

class MockSnapshot;
using MockSnapshotPtr = std::shared_ptr<MockSnapshot>;
class MockSnapshot
{
private:
    std::shared_ptr<MockEntries> entries;

public:
    MockSnapshot()
        : entries(std::make_shared<MockEntries>())
    {}
    std::shared_ptr<MockEntries> version() { return entries; }

    static MockSnapshotPtr createFrom(std::vector<std::pair<PageId, PageEntry>> && entries)
    {
        auto snap = std::make_shared<MockSnapshot>();
        for (const auto & [pid, entry] : entries)
        {
            snap->entries->put(pid, entry);
        }
        return snap;
    }
};

} // namespace DB::PS::V2::tests

#endif // NDEBUG

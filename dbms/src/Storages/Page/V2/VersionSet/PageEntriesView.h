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

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V2/PageEntries.h>

#include <optional>

namespace DB::PS::V2
{
/// Treat a list of PageEntriesForDelta as a single PageEntries
class PageEntriesView
{
private:
    // tail of the list
    PageEntriesForDeltaPtr tail;

public:
    explicit PageEntriesView(PageEntriesForDeltaPtr tail_)
        : tail(std::move(tail_))
    {}

    std::optional<PageEntry> find(PageId page_id) const;

    PageEntry at(PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId> validNormalPageIds() const;
    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const;

    PageId maxId() const;

    inline PageEntriesForDeltaPtr getSharedTailVersion() const { return tail; }

    void release() { tail.reset(); }

    size_t numPages() const;
    size_t numNormalPages() const;

    PageId resolveRefId(PageId page_id) const;

private:
    friend class DeltaVersionEditAcceptor;
};

} // namespace DB::PS::V2

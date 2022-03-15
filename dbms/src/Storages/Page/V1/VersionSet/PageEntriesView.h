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

#include <optional>

namespace DB::PS::V1
{
/// Treat a list of PageEntriesForDelta as a single PageEntries
class PageEntriesView
{
private:
    // tail of the list
    std::shared_ptr<PageEntriesForDelta> tail;

public:
    explicit PageEntriesView(std::shared_ptr<PageEntriesForDelta> tail_)
        : tail(std::move(tail_))
    {}

    std::optional<PageEntry> find(PageId page_id) const;

    const PageEntry at(PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId> validNormalPageIds() const;
    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const;

    PageId maxId() const;

    inline std::shared_ptr<PageEntriesForDelta> getSharedTailVersion() const { return tail; }

    inline std::shared_ptr<PageEntriesForDelta> transferTailVersionOwn()
    {
        std::shared_ptr<PageEntriesForDelta> owned_ptr;
        owned_ptr.swap(tail);
        return owned_ptr;
    }

    size_t numPages() const;
    size_t numNormalPages() const;

private:
    PageId resolveRefId(PageId page_id) const;

    friend class DeltaVersionEditAcceptor;
};

} // namespace DB::PS::V1

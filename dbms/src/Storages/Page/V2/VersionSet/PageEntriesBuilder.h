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

#include <Storages/Page/V2/PageEntries.h>
#include <Storages/Page/V2/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/WriteBatchImpl.h>

namespace DB::PS::V2
{
class PageEntriesBuilder
{
public:
    explicit PageEntriesBuilder(
        const PageEntries * old_version_,
        bool ignore_invalid_ref_ = false,
        LoggerPtr log_ = nullptr)
        : old_version(const_cast<PageEntries *>(old_version_))
        , current_version(new PageEntries)
        , ignore_invalid_ref(ignore_invalid_ref_)
        , log(log_)
    {
#ifndef NDEBUG
        if (ignore_invalid_ref)
        {
            assert(log != nullptr);
        }
#endif
        old_version->increase();
        current_version->copyEntries(*old_version);
    }

    ~PageEntriesBuilder() { old_version->release(); }

    void apply(const PageEntriesEdit & edit);

    void gcApply(PageEntriesEdit & edit) { gcApplyTemplate(current_version, edit, current_version); }

    PageEntries * build() { return current_version; }

public:
    template <typename OldVersionType, typename VersionType>
    static void gcApplyTemplate(const OldVersionType & old_version, PageEntriesEdit & edit, VersionType & new_version)
    {
        for (auto & rec : edit.getRecords())
        {
            if (unlikely(rec.type == WriteBatchWriteType::PUT))
                throw Exception("Should use UPDATE for gc edits, please check your code!!", ErrorCodes::LOGICAL_ERROR);

            if (rec.type != WriteBatchWriteType::UPSERT)
                continue;
            // Gc only apply MOVE_NORMAL_PAGE for updating normal page entries
            const auto old_page_entry = old_version->findNormalPageEntry(rec.page_id);
            // If the gc page have already been removed, or is a ref to non-exist page, just ignore it
            if (!old_page_entry)
                continue;
            // In case of page being updated during GC process.
            if (old_page_entry->fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                rec.entry.ref = old_page_entry->ref;
                new_version->normal_pages[rec.page_id] = rec.entry;
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
    }

private:
    PageEntries * old_version;
    PageEntries * current_version;
    bool ignore_invalid_ref;
    LoggerPtr log;
};

} // namespace DB::PS::V2

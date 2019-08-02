#pragma once

#include <Storages/Page/PageEntries.h>
#include <Storages/Page/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

class PageEntriesBuilder
{
public:
    explicit PageEntriesBuilder(const PageEntries * old_version_, //
                                bool                ignore_invalid_ref_ = false,
                                Poco::Logger *      log_                = nullptr)
        : old_version(const_cast<PageEntries *>(old_version_)),
          current_version(new PageEntries), //
          ignore_invalid_ref(ignore_invalid_ref_),
          log(log_)
    {
#ifndef NDEBUG
        if (ignore_invalid_ref)
        {
            assert(log != nullptr);
        }
#endif
        old_version->incrRefCount();
        current_version->copyEntries(*old_version);
    }

    ~PageEntriesBuilder() { old_version->decrRefCount(); }

    void apply(const PageEntriesEdit & edit);

    void gcApply(PageEntriesEdit & edit) { gcApplyTemplate(current_version, edit, current_version); }

    PageEntries * build() { return current_version; }

public:
    template <typename OldVersionType, typename VersionType>
    static void gcApplyTemplate(const OldVersionType & old_version, PageEntriesEdit & edit, VersionType & new_version)
    {
        for (auto & rec : edit.getRecords())
        {
            if (rec.type != WriteBatch::WriteType::PUT)
                continue;
            // Gc only apply PUT for updating page entries
            auto old_page_entry = old_version->find(rec.page_id);
            // If the gc page have already been removed, or is a ref to non-exist page, just ignore it
            if (old_page_entry == nullptr)
                continue;
            // In case of page being updated during GC process.
            if (old_page_entry->fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                rec.entry.ref                          = old_page_entry->ref;
                new_version->normal_pages[rec.page_id] = rec.entry;
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
    }

private:
    PageEntries *  old_version;
    PageEntries *  current_version;
    bool           ignore_invalid_ref;
    Poco::Logger * log;
};

} // namespace DB

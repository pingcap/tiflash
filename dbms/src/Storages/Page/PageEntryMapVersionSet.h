#pragma once

#include <set>
#include <vector>

#include <Common/VersionSet.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    void put(PageId page_id, const PageEntry & entry)
    {
        EditRecord record;
        record.type    = WriteBatch::WriteType::PUT;
        record.page_id = page_id;
        record.entry   = entry;
        records.emplace_back(record);
    }

    void del(PageId page_id)
    {
        EditRecord record;
        record.type    = WriteBatch::WriteType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageId ref_id, PageId page_id)
    {
        EditRecord record;
        record.type        = WriteBatch::WriteType::REF;
        record.page_id     = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        WriteBatch::WriteType type;
        PageId                page_id;
        PageEntry             entry;
        PageId                ori_page_id;
    };
    using EditRecords = std::vector<EditRecord>;

    const EditRecords & getRecords() const { return records; }

private:
    EditRecords records;

public:
    // No copying allowed
    PageEntriesEdit(const PageEntriesEdit &) = delete;
    PageEntriesEdit & operator=(const PageEntriesEdit &) = delete;
    // Only move allowed
    PageEntriesEdit(PageEntriesEdit && rhs) noexcept : PageEntriesEdit() { *this = std::move(rhs); }
    PageEntriesEdit & operator=(PageEntriesEdit && rhs) noexcept
    {
        if (this != &rhs)
        {
            records.swap(rhs.records);
        }
        return *this;
    }
};

class PageEntryMapBuilder
{
public:
    PageEntryMapBuilder(PageEntryMap * base) : v(new PageEntryMap) { v->copyEntries(*base); }

    void apply(const PageEntriesEdit & edit)
    {
        for (const auto & rec : edit.getRecords())
        {
            switch (rec.type)
            {
            case WriteBatch::WriteType::PUT:
                v->put(rec.page_id, rec.entry);
                break;
            case WriteBatch::WriteType::DEL:
                v->del(rec.page_id);
                break;
            case WriteBatch::WriteType::REF:
                v->ref(rec.page_id, rec.ori_page_id);
                break;
            }
        }
    }

    void gcApply(const PageEntriesEdit & edit)
    {
        for (const auto & rec : edit.getRecords())
        {
            if (rec.type != WriteBatch::WriteType::PUT)
            {
                continue;
            }
            // Gc only apply PUT for updating page entries
            auto old_iter = v->find(rec.page_id);
            // If the gc page have already been removed, just ignore it
            if (old_iter == v->end())
            {
                continue;
            }
            auto & old_page_entry = old_iter.pageEntry();
            // In case of page being updated during GC process.
            if (old_page_entry.fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                old_page_entry = rec.entry;
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
    }

    PageEntryMap * build() { return v; }

private:
    PageEntryMap * v;
};

class PageEntryMapVersionSet : public ::DB::MVCC::VersionSet<PageEntryMap, PageEntriesEdit, PageEntryMapBuilder>
{
public:
    using SnapshotPtr = ::DB::MVCC::VersionSet<PageEntryMap, PageEntriesEdit, PageEntryMapBuilder>::SnapshotPtr;

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    std::set<PageFileIdAndLevel> gcApply(const PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;
};

} // namespace DB

#pragma once

#include <set>
#include <vector>

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

class PageEntryMapVersionSet
{
public:
    PageEntryMapVersionSet() : dummy_versions(), current_map(nullptr) { appendVersion(new PageEntryMap); }
    ~PageEntryMapVersionSet()
    {
        current_map->decrRefCount();
        assert(dummy_versions.next == &dummy_versions); // List must be empty
    }

    /// current version
    PageEntryMap * currentMap() const { return current_map; }

    /// `apply` accept changes and append new version to version-list
    void apply(const PageEntriesEdit & edit);

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    void gcApply(const PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;

private:
    void appendVersion(PageEntryMap * v);

    size_t getVersionSetSize();

private:
    PageEntryMap   dummy_versions; // Head of circular double-linked list of all PageEntryMap
    PageEntryMap * current_map;    // == dummy_versions.prev
};

} // namespace DB

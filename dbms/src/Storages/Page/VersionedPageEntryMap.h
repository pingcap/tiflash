#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/WriteBatch.h>
#include <vector>

namespace DB
{

class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    void put(PageId page_id, const PageEntry & entry)
    {
        records.emplace_back( //
            EditRecord{.type = WriteBatch::WriteType::PUT, .page_id = page_id, .entry = entry});
    }

    void del(PageId page_id)
    {
        records.emplace_back( //
            EditRecord{.type = WriteBatch::WriteType::DEL, .page_id = page_id});
    }

    void ref(PageId ref_id, PageId page_id)
    {
        records.emplace_back( //
            EditRecord{.type = WriteBatch::WriteType::REF, .page_id = ref_id, .ori_page_id = page_id});
    }

    bool empty() const { return records.empty(); }

    struct EditRecord
    {
        WriteBatch::WriteType type;
        PageId                page_id;
        union
        {
            PageEntry entry;
            PageId    ori_page_id;
        };
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

class VersionedPageEntryMap
{
public:
    VersionedPageEntryMap() : dummy_versions(), current_map(nullptr) { AppendVersion(new PageEntryMap); }
    ~VersionedPageEntryMap()
    {
        current_map->decrRefCount();
        assert(dummy_versions.next == &dummy_versions); // List must be empty
    }

    PageEntryMap * currentMap() const { return current_map; }

    void apply(const PageEntriesEdit & edit)
    {
        PageEntryMap * base = current_map;
        base->incrRefCount();

        auto * v = new PageEntryMap;
        v->copyEntries(*base);
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

        base->decrRefCount();

        AppendVersion(v);
    }

    void gcApply(const PageEntriesEdit & edit)
    {
        PageEntryMap * base = current_map;
        base->incrRefCount();

        auto * v = new PageEntryMap;
        v->copyEntries(*base);
        for (const auto & rec : edit.getRecords())
        {
            if (rec.type != WriteBatch::WriteType::PUT)
            {
                continue;
            }
            // Apply gc only accept PUT
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

        base->decrRefCount();

        AppendVersion(v);
    }

private:
    void AppendVersion(PageEntryMap * v)
    {
        // Make "v" become "current_map"
        assert(v->ref_count == 0);
        assert(v != current_map);
        if (current_map != nullptr)
        {
            current_map->decrRefCount();
        }
        current_map = v;
        current_map->incrRefCount();

        // Append to linked list
        current_map->prev       = dummy_versions.prev;
        current_map->next       = &dummy_versions;
        current_map->prev->next = current_map;
        current_map->next->prev = current_map;
    }

private:
    PageEntryMap   dummy_versions; // Head of circular double-linked list of all PageEntryMap
    PageEntryMap * current_map;    // == dummy_versions.prev
};

} // namespace DB

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

void PageEntryMapVersionSet::apply(const PageEntriesEdit & edit)
{

    PageEntryMap * base = current_map;
    base->incrRefCount();

    auto * v = new PageEntryMap;
    v->copyEntries(*base); // maybe expensive if there are millions of pages?
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

    appendVersion(v);
}

void PageEntryMapVersionSet::gcApply(const PageEntriesEdit & edit)
{
    PageEntryMap * base = current_map;
    base->incrRefCount();

    auto * v = new PageEntryMap;
    v->copyEntries(*base); // maybe expensive if there are millions of pages?
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

    base->decrRefCount();

    appendVersion(v);
}

std::set<PageFileIdAndLevel> PageEntryMapVersionSet::listAllLiveFiles() const
{
    std::set<PageFileIdAndLevel> liveFiles;
    for (PageEntryMap * v = dummy_versions.next; v != &dummy_versions; v = v->next)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            liveFiles.insert(it->second.fileIdLevel());
        }
    }
    return liveFiles;
}

size_t PageEntryMapVersionSet::getVersionSetSize()
{
    size_t size = 0;
    for (PageEntryMap * v = dummy_versions.next; v != &dummy_versions; v = v->next)
        size += 1;
    return size;
}

void PageEntryMapVersionSet::appendVersion(PageEntryMap * const v)
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

} // namespace DB

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

std::set<PageFileIdAndLevel> PageEntryMapVersionSet::gcApply(const PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    PageEntryMap * base = current;
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

    this->appendVersion(v);

    return listAllLiveFiles();
}

std::set<PageFileIdAndLevel> PageEntryMapVersionSet::listAllLiveFiles() const
{
    std::set<PageFileIdAndLevel> liveFiles;
    for (PageEntryMap * v = placeholder_node.next; v != &placeholder_node; v = v->next)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            liveFiles.insert(it->second.fileIdLevel());
        }
    }
    return liveFiles;
}

} // namespace DB

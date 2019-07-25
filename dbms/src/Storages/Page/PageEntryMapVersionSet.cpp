#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

std::set<PageFileIdAndLevel> PageEntryMapVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    // apply edit on base
    PageEntryMap * v = nullptr;
    {
        PageEntryMapBuilder builder(current);
        builder.gcApply(edit);
        v = builder.build();
    }

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

void PageEntryMapBuilder::apply(const PageEntriesEdit & edit)
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
            if (likely(!ignore_invalid_ref))
            {
                v->ref<false>(rec.page_id, rec.ori_page_id);
            }
            else
            {
                try
                {
                    v->ref<true>(rec.page_id, rec.ori_page_id);
                }
                catch (DB::Exception & e)
                {
                    LOG_WARNING(log, "Ignore invalid RefPage while opening PageStorage: " + e.message());
                }
            }
            break;
        }
    }
}

void PageEntryMapBuilder::gcApply(PageEntriesEdit & edit)
{
    for (auto & rec : edit.getRecords())
    {
        if (rec.type != WriteBatch::WriteType::PUT)
            continue;
        // Gc only apply PUT for updating page entries
        try
        {
            auto old_page_entry = v->find(rec.page_id);
            // If the gc page have already been removed, just ignore it
            if (old_page_entry == nullptr)
                continue;
            // In case of page being updated during GC process.
            if (old_page_entry->fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                rec.entry.ref = old_page_entry->ref;
                v->normal_pages[rec.page_id] = rec.entry;
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
        catch (DB::Exception & e)
        {
            // just ignore and continue
        }
    }
}


} // namespace DB

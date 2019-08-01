#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>

namespace DB
{

std::set<PageFileIdAndLevel> PageEntriesVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    // apply edit on base
    PageEntries * v = nullptr;
    {
        PageEntriesBuilder builder(current);
        builder.gcApply(edit);
        v = builder.build();
    }

    this->appendVersion(v);

    return listAllLiveFiles();
}

std::set<PageFileIdAndLevel> PageEntriesVersionSet::listAllLiveFiles() const
{
    std::set<PageFileIdAndLevel> liveFiles;
    for (PageEntries * v = placeholder_node.next; v != &placeholder_node; v = v->next)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            liveFiles.insert(it->second.fileIdLevel());
        }
    }
    return liveFiles;
}

void PageEntriesBuilder::apply(const PageEntriesEdit & edit)
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


} // namespace DB

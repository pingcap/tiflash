#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

std::set<PageFileIdAndLevel> PageEntryMapVersionSet::gcApply(const PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    // apply edit on base
    PageEntryMap * base = current;
    base->incrRefCount();
    PageEntryMapBuilder builder(base);
    builder.gcApply(edit);
    PageEntryMap * v = builder.build();
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

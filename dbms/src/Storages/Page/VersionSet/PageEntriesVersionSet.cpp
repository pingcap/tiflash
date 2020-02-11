#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>

namespace DB
{

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> PageEntriesVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_write_mutex);

    // apply edit on base
    PageEntries * v = nullptr;
    {
        PageEntriesBuilder builder(current);
        builder.gcApply(edit);
        v = builder.build();
    }

    this->appendVersion(v, lock);

    return listAllLiveFiles(lock);
}

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>>
PageEntriesVersionSet::listAllLiveFiles(const std::unique_lock<std::shared_mutex> & lock) const
{
    (void)lock;
    std::set<PageFileIdAndLevel> live_files;
    std::set<PageId>             live_normal_pages;
    for (PageEntries * v = placeholder_node.next; v != &placeholder_node; v = v->next)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            live_normal_pages.insert(it->first);
            live_files.insert(it->second.fileIdLevel());
        }
    }
    return {live_files, live_normal_pages};
}


} // namespace DB

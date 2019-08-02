#include <Storages/Page/VersionSet/PageEntriesVersionSetWithDelta.h>

namespace DB
{

////  PageEntryMapView

const PageEntry * PageEntriesView::find(PageId page_id) const
{
    // First we find ref-pairs to get the normal page id
    bool   found          = false;
    PageId normal_page_id = 0;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        if (node->isRefDeleted(page_id))
        {
            return nullptr;
        }

        auto iter = node->page_ref.find(page_id);
        if (iter != node->page_ref.end())
        {
            found          = true;
            normal_page_id = iter->second;
            break;
        }
    }
    if (!found)
    {
        // The page have been deleted.
        return nullptr;
    }

    auto entry = findNormalPageEntry(normal_page_id);
    // RefPage exists, but normal Page do NOT exist. Should NOT call here
    if (entry == nullptr)
    {
        throw DB::Exception("Accessing RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                            ErrorCodes::LOGICAL_ERROR);
    }
    return entry;
}

const PageEntry & PageEntriesView::at(const PageId page_id) const
{
    auto entry = this->find(page_id);
    if (entry == nullptr)
    {
        throw DB::Exception("Accessing non-exist Page[" + DB::toString(page_id) + "]", ErrorCodes::LOGICAL_ERROR);
    }
    return *entry;
}

const PageEntry * PageEntriesView::findNormalPageEntry(PageId page_id) const
{
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        auto iter = node->normal_pages.find(page_id);
        if (iter != node->normal_pages.end())
        {
            return &iter->second;
        }
    }
    return nullptr;
}

std::pair<bool, PageId> PageEntriesView::isRefId(PageId page_id) const
{
    auto node = tail;
    for (; !node->isBase(); node = node->prev)
    {
        if (node->ref_deletions.count(page_id) > 0)
            return {false, 0};
        auto iter = node->page_ref.find(page_id);
        if (iter != node->page_ref.end())
            return {true, iter->second};
    }
    return node->isRefId(page_id);
}

PageId PageEntriesView::resolveRefId(PageId page_id) const
{
    auto [is_ref, normal_page_id] = isRefId(page_id);
    return is_ref ? normal_page_id : page_id;
}

std::set<PageId> PageEntriesView::validPageIds() const
{
    std::stack<std::shared_ptr<PageEntriesForDelta>> link_nodes;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        link_nodes.emplace(node);
    }
    // Get valid pages, from link-list's head to tail
    std::set<PageId> valid_pages;
    while (!link_nodes.empty())
    {
        auto node = link_nodes.top();
        link_nodes.pop();
        if (!node->isBase())
        {
            for (auto deleted_id : node->ref_deletions)
            {
                valid_pages.erase(deleted_id);
            }
        }
        for (auto ref_pairs : node->page_ref)
        {
            valid_pages.insert(ref_pairs.first);
        }
    }
    return valid_pages;
}

std::set<PageId> PageEntriesView::validNormalPageIds() const
{
    std::stack<std::shared_ptr<PageEntriesForDelta>> link_nodes;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        link_nodes.emplace(node);
    }
    // Get valid normal pages, from link-list's head to tail
    std::set<PageId> valid_normal_pages;
    while (!link_nodes.empty())
    {
        auto node = link_nodes.top();
        link_nodes.pop();
        if (!node->isBase())
        {
            for (auto deleted_id : node->ref_deletions)
            {
                valid_normal_pages.erase(deleted_id);
            }
        }
        for (auto & [page_id, entry] : node->normal_pages)
        {
            if (entry.ref != 0)
                valid_normal_pages.insert(page_id);
        }
    }
    return valid_normal_pages;
}

PageId PageEntriesView::maxId() const
{
    PageId max_id = 0;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        max_id = std::max(max_id, node->maxId());
    }
    return max_id;
}

} // namespace DB

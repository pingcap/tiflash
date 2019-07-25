#include <Storages/Page/PageEntryMapDeltaVersionSet.h>

namespace DB
{

////  PageEntryMapView

const PageEntry * PageEntryMapView::find(PageId page_id) const
{
    // First we find ref-pairs to get the normal page id
    bool found = false;
    PageId normal_page_id = 0;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        if (node->isDeleted(page_id))
        {
            return nullptr;
        }

        auto iter = node->page_ref.find(page_id);
        if (iter != node->page_ref.end())
        {
            // if ref-id find in this delta, turn to find  in this VersionView
            found = true;
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
        throw DB::Exception(
                "Accessing RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                ErrorCodes::LOGICAL_ERROR);
    }
    return entry;
}

const PageEntry & PageEntryMapView::at(const PageId page_id) const
{
    auto entry = this->find(page_id);
    if (entry == nullptr)
    {
        throw DB::Exception("Accessing non-exist Page[" + DB::toString(page_id) + "]", ErrorCodes::LOGICAL_ERROR);
    }
    return *entry;
}

const PageEntry * PageEntryMapView::findNormalPageEntry(PageId page_id) const
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

std::pair<bool, PageId> PageEntryMapView::isRefId(PageId page_id) const
{
    auto node = tail;
    for (; !node->isBase(); node = node->prev)
    {
        auto [is_ref, ori_id] = node->isRefId(page_id);
        if (is_ref)
            return {is_ref, ori_id};
    }
    return node->isRefId(page_id);
}

PageId PageEntryMapView::resolveRefId(PageId page_id) const
{
    auto [is_ref, normal_page_id] = isRefId(page_id);
    return is_ref ? normal_page_id : page_id;
}

std::set<PageId> PageEntryMapView::validPageIds() const
{
    std::vector<std::shared_ptr<PageEntryMapBase>> link_nodes;
    for (auto node = vset->current; node != nullptr; node = node->prev)
    {
        link_nodes.emplace_back(node);
    }
    // Get valid pages, from link-list's head to tail
    std::set<PageId> valid_pages;
    for (auto node_iter = link_nodes.rbegin(); node_iter != link_nodes.rend(); node_iter++)
    {
        if (!(*node_iter)->isBase())
        {
            for (auto deleted_id : (*node_iter)->ref_deletions)
            {
                valid_pages.erase(deleted_id);
            }
        }
        for (auto ref_pairs : (*node_iter)->page_ref)
        {
            valid_pages.insert(ref_pairs.first);
        }
    }
    return valid_pages;
}

std::set<PageId> PageEntryMapView::validNormalPageIds() const
{
    std::vector<std::shared_ptr<PageEntryMapBase>> link_nodes;
    for (auto node = vset->current; node != nullptr; node = node->prev)
    {
        link_nodes.emplace_back(node);
    }
    // Get valid normal pages, from link-list's head to tail
    std::set<PageId> valid_normal_pages;
    for (auto node_iter = link_nodes.rbegin(); node_iter != link_nodes.rend(); node_iter++)
    {
        if (!(*node_iter)->isBase())
        {
            for (auto deleted_id : (*node_iter)->ref_deletions)
            {
                valid_normal_pages.erase(deleted_id);
            }
        }
        for (auto ref_pairs : (*node_iter)->normal_pages)
        {
            valid_normal_pages.insert(ref_pairs.first);
        }
    }
    return valid_normal_pages;
}

PageId PageEntryMapView::maxId() const
{
    PageId max_id = 0;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        max_id = std::max(max_id, node->maxId());
    }
    return max_id;
}

} // namespace DB

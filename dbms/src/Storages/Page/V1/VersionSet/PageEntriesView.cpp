#include <Storages/Page/V1/VersionSet/PageEntriesVersionSetWithDelta.h>

namespace DB::PS::V1
{
////  PageEntryMapView

std::optional<PageEntry> PageEntriesView::find(PageId page_id) const
{
    // First we find ref-pairs to get the normal page id
    bool found = false;
    PageId normal_page_id = 0;
    for (PageEntriesForDeltaPtr node = tail; node != nullptr; node = std::atomic_load(&node->prev))
    {
        if (node->isRefDeleted(page_id))
        {
            return std::nullopt;
        }

        auto iter = node->page_ref.find(page_id);
        if (iter != node->page_ref.end())
        {
            found = true;
            normal_page_id = iter->second;
            break;
        }
    }
    if (!found)
    {
        // The page have been deleted.
        return std::nullopt;
    }

    auto entry = findNormalPageEntry(normal_page_id);
    // RefPage exists, but normal Page do NOT exist. Should NOT call here
    if (!entry)
    {
        throw DB::Exception("Accessing RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                            ErrorCodes::LOGICAL_ERROR);
    }
    return entry;
}

const PageEntry PageEntriesView::at(const PageId page_id) const
{
    auto entry = this->find(page_id);
    if (!entry)
    {
        throw DB::Exception("Accessing non-exist Page[" + DB::toString(page_id) + "]", ErrorCodes::LOGICAL_ERROR);
    }
    return *entry;
}

std::optional<PageEntry> PageEntriesView::findNormalPageEntry(PageId page_id) const
{
    for (PageEntriesForDeltaPtr node = tail; node != nullptr; node = std::atomic_load(&node->prev))
    {
        auto iter = node->normal_pages.find(page_id);
        if (iter != node->normal_pages.end())
        {
            return iter->second;
        }
    }
    return std::nullopt;
}

std::pair<bool, PageId> PageEntriesView::isRefId(PageId page_id) const
{
    auto node = tail;
    // For delta, we need to check if page_id is deleted, then try to find in page_ref
    for (; !node->isBase(); node = std::atomic_load(&node->prev))
    {
        if (node->ref_deletions.count(page_id) > 0)
            return {false, 0};
        auto iter = node->page_ref.find(page_id);
        if (iter != node->page_ref.end())
            return {true, iter->second};
    }
    // For base
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
    for (auto node = tail; node != nullptr; node = std::atomic_load(&node->prev))
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
    // TODO: add test cases for this function
    std::stack<std::shared_ptr<PageEntriesForDelta>> link_nodes;
    for (auto node = tail; node != nullptr; node = std::atomic_load(&node->prev))
    {
        link_nodes.emplace(node);
    }
    // Get valid normal pages, from link-list's head to tail
    std::set<PageId> valid_normal_pages;
    while (!link_nodes.empty())
    {
        auto node = link_nodes.top();
        link_nodes.pop();
        for (auto & [page_id, entry] : node->normal_pages)
        {
            if (entry.isTombstone())
            {
                valid_normal_pages.erase(page_id);
            }
            else
            {
                valid_normal_pages.insert(page_id);
            }
        }
    }
    return valid_normal_pages;
}

PageId PageEntriesView::maxId() const
{
    PageId max_id = 0;
    for (auto node = tail; node != nullptr; std::atomic_load(&node->prev))
    {
        max_id = std::max(max_id, node->maxId());
    }
    return max_id;
}

size_t PageEntriesView::numPages() const
{
    std::unordered_set<PageId> page_ids;
    std::vector<std::shared_ptr<PageEntriesForDelta>> nodes;
    for (auto node = tail; node != nullptr; node = std::atomic_load(&node->prev))
        nodes.emplace_back(node);

    for (auto node = nodes.rbegin(); node != nodes.rend(); ++node)
    {
        for (const auto & pair : (*node)->page_ref)
        {
            page_ids.insert(pair.first);
        }
        for (const auto & page_id_to_del : (*node)->ref_deletions)
        {
            page_ids.erase(page_id_to_del);
        }
    }

    return page_ids.size();
}

size_t PageEntriesView::numNormalPages() const
{
    auto normal_ids = validNormalPageIds();
    return normal_ids.size();
}

} // namespace DB::PS::V1

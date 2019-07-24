#include <Storages/Page/PageEntryMapDeltaVersionSet.h>

#include <stack>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

////  PageEntryMapDeltaVersionSet

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    if (current.use_count() == 1 && current->isBase())
    {
        // If no readers, we could directly merge edits
        BuilderType::gcApplyInplace(current, edit);
    }
    else
    {
        if (current.use_count() != 1)
        {
            VersionPtr v = VersionType::createDelta();
            appendVersion(std::move(v));
        }
        auto                     view = std::make_shared<PageEntryMapView>(this, current);
        PageEntryMapDeltaBuilder builder(view.get());
        builder.gcApply(edit);
    }

    return listAllLiveFiles();
}

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::listAllLiveFiles() const
{
    // Note read_mutex must be hold.
    std::set<PageFileIdAndLevel> liveFiles;
    // Iterate all snapshot to collect all PageFile in used.
    for (auto s = snapshots->next; s != snapshots.get(); s = s->next)
    {
        for (auto v = s->version()->tail; v != nullptr; v = v->prev)
        {
            for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
            {
                liveFiles.insert(it->second.fileIdLevel());
            }
        }
    }
    return liveFiles;
}

////  PageEntryMapDeltaBuilder

PageEntryMapDeltaBuilder::PageEntryMapDeltaBuilder(const PageEntryMapView * base_, bool ignore_invalid_ref_, Logger * log_)
    : base(const_cast<PageEntryMapView *>(base_)),
      v(base->tail),
      ignore_invalid_ref(ignore_invalid_ref_),
      log(log_)
{
#ifndef NDEBUG
    if (ignore_invalid_ref)
    {
        assert(log != nullptr);
    }
#endif
}

PageEntryMapDeltaBuilder::~PageEntryMapDeltaBuilder() = default;

/// Apply edits and generate new delta
void PageEntryMapDeltaBuilder::apply(PageEntriesEdit & edit)
{
    for (auto && rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT:
        {
            v->ref_deletions.erase(rec.page_id);
            const PageId normal_page_id = base->resolveRefId(rec.page_id);
            bool is_ref_exist = base->isRefExists(rec.page_id, normal_page_id);
            if (!is_ref_exist)
            {
                v->page_ref.emplace(rec.page_id, normal_page_id);
            }

            auto old_entry = base->findNormalPageEntry(normal_page_id);
            assert(!is_ref_exist || (is_ref_exist && old_entry != nullptr));
            if (old_entry == nullptr)
            {
                rec.entry.ref = 1;
                v->normal_pages[normal_page_id] = rec.entry;
            }
            else
            {
                rec.entry.ref = old_entry->ref + !is_ref_exist;
                v->normal_pages[normal_page_id] = rec.entry;
            }

            v->max_page_id = std::max(v->max_page_id, rec.page_id);
            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            v->ref_deletions.insert(rec.page_id);
            v->page_ref.erase(rec.page_id);
            const PageId normal_page_id = base->resolveRefId(rec.page_id);
            this->decreasePageRef(normal_page_id);
            break;
        }
        case WriteBatch::WriteType::REF:
        {
            v->ref_deletions.erase(rec.page_id);
            // Shorten ref-path in case there is RefPage to RefPage
            const PageId normal_page_id = base->resolveRefId(rec.ori_page_id);
            auto old_entry = base->findNormalPageEntry(normal_page_id);
            if (likely(old_entry != nullptr))
            {
                auto [is_ref_id, old_normal_id] = base->isRefId(rec.page_id);
                if (unlikely(is_ref_id))
                {
                    if (old_normal_id == normal_page_id)
                        break;
                    this->decreasePageRef(old_normal_id);
                }
                v->page_ref[rec.page_id] = normal_page_id;
                // increase entry's ref-count
                auto new_entry = *old_entry;
                new_entry.ref += 1;
                v->normal_pages[rec.page_id] = new_entry;
            }
            else
            {
                if (ignore_invalid_ref)
                {
                    LOG_WARNING(log, "Ignore invalid RefPage while opening PageStorage: RefPage" + DB::toString(rec.page_id) + " to non-exist Page" + DB::toString(rec.ori_page_id));
                }
                else
                {
                    v->page_ref[rec.page_id] = normal_page_id;
                }
            }
            v->max_page_id = std::max(v->max_page_id, rec.page_id);
            break;
        }
        }
    }
}

void PageEntryMapDeltaBuilder::applyInplace(const PageEntryMapDeltaVersionSet::VersionPtr & current, const PageEntriesEdit & edit)
{
    assert(current->isBase());
    for (auto && rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT:
            current->put(rec.page_id, rec.entry);
            break;
        case WriteBatch::WriteType::DEL:
            current->del(rec.page_id);
            break;
        case WriteBatch::WriteType::REF:
            // Shorten ref-path in case there is RefPage to RefPage
            current->ref(rec.page_id, rec.ori_page_id);
            break;
        }
    }
}

void PageEntryMapDeltaBuilder::gcApply(PageEntriesEdit & edit)
{
    for (auto & rec : edit.getRecords())
    {
        if (rec.type != WriteBatch::WriteType::PUT)
        {
            continue;
        }
        // Gc only apply PUT for updating page entries
        try
        {
            auto old_page_entry = base->find(rec.page_id); // this may throw an exception if ref to non-exist page
            // If the gc page have already been removed, just ignore it
            if (old_page_entry == nullptr)
            {
                continue;
            }
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

void PageEntryMapDeltaBuilder::gcApplyInplace(const PageEntryMapDeltaVersionSet::VersionPtr & current, const PageEntriesEdit & edit)
{
    for (const auto & rec : edit.getRecords())
    {
        if (rec.type != WriteBatch::WriteType::PUT)
        {
            continue;
        }
        // Gc only apply PUT for updating page entries
        try
        {
            auto old_page_entry = current->find(rec.page_id); // this may throw an exception if ref to non-exist page
            // If the gc page have already been removed, just ignore it
            if (old_page_entry == nullptr)
            {
                continue;
            }
            // In case of page being updated during GC process.
            if (old_page_entry->fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                current->put(rec.page_id, rec.entry, false);
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
        catch (DB::Exception & e)
        {
            // just ignore and continue
        }
    }
}

PageEntryMapDeltaVersionSet::VersionPtr
PageEntryMapDeltaBuilder::compactDeltaAndBase(const PageEntryMapDeltaVersionSet::VersionPtr & old_base,
                                              PageEntryMapDeltaVersionSet::VersionPtr &       delta)
{
    PageEntryMapDeltaVersionSet::VersionPtr base = PageEntryMapBase::createBase();
    base->copyEntries(*old_base);
    // apply delta edits
    delta->prev = base;
    base->merge(*delta);
    delta->clear();
    return base;
}

PageEntryMapDeltaVersionSet::VersionPtr PageEntryMapDeltaBuilder::compactDeltas(const PageEntryMapDeltaVersionSet::VersionPtr & tail)
{
    if (tail->prev == nullptr || tail->prev->isBase())
    {
        // Only one delta, do nothing
        return nullptr;
    }

    auto tmp = PageEntryMapDeltaVersionSet::VersionType::createDelta();

    std::stack<PageEntryMapDeltaVersionSet::VersionPtr> nodes;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        if (node->isBase())
        {
            // link `tmp` to `base` version
            tmp->prev = node;
        }
        else
        {
            nodes.push(node);
        }
    }
    // merge delta forward
    while (!nodes.empty())
    {
        auto node = nodes.top();
        nodes.pop();
        tmp->merge(*node);
    }

    return tmp;
}

void PageEntryMapDeltaBuilder::decreasePageRef(const PageId page_id)
{
    auto old_entry = base->findNormalPageEntry(page_id);
    if (old_entry != nullptr)
    {
        auto entry = *old_entry;
        entry.ref = old_entry->ref <= 1? 0: old_entry->ref - 1;
        // keep an entry of ref-count == 0, so that we can delete this entry when merged to base
        v->normal_pages[page_id] = entry;
    }
}

////  PageEntryMapView

PageId PageEntryMapView::maxId() const
{
    PageId max_id = 0;
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        max_id = std::max(max_id, node->maxId());
    }
    return max_id;
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
            // if new ref find in this delta, turn to find ori_page_id in this VersionView
            found = true;
            normal_page_id = iter->second;
            break;
        }
    }
    if (!found)
    {
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

bool PageEntryMapView::isRefExists(PageId ref_id, PageId page_id) const
{
    auto node = tail;
    for (; !node->isBase(); node = node->prev)
    {
        // `ref_id` or `page_id` has been deleted in later version, then return not exist
        if (node->isDeleted(ref_id))
        {
            return false;
        }
        auto [is_ref, ori_ref_id] = node->isRefId(ref_id);
        if (is_ref)
        {
            // if `ref_id` find in this delta
            // find ref pair in this delta
            if (ori_ref_id == page_id)
            {
                return true;
            }
            // turn to find if `ori_page_id` -> `page_id` is exists
            ref_id = ori_ref_id;
        }
        auto [is_page_id_ref, ori_page_id] = node->isRefId(page_id);
        if (is_page_id_ref)
        {
            // turn to find if `ref_id` -> `ori_page_id` is exists
            page_id = ori_page_id;
        }
    }
    assert(node->isBase());
    return node->isRefExists(ref_id, page_id);
}

std::pair<bool, PageId> PageEntryMapView::isRefId(PageId page_id)
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
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        if (node->isDeleted(page_id))
        {
            return false;
        }
        auto [is_ref, ori_ref_id] = node->isRefId(page_id);
        if (is_ref)
        {
            return ori_ref_id;
        }
        if (node->isBase())
        {
            return page_id;
        }
    }
    assert(false);
    // should not call here
    return 0;
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

} // namespace DB
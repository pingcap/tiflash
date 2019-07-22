#include <Storages/Page/PageEntryMapDeltaVersionSet.h>

#include <stack>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

PageEntryMapDeltaBuilder::PageEntryMapDeltaBuilder(const PageEntryMapView * base_, bool ignore_invalid_ref_, Logger * log_)
    : base(const_cast<PageEntryMapView *>(base_)), v(PageEntryMapDelta::createDelta()), ignore_invalid_ref(ignore_invalid_ref_), log(log_)
{
#ifndef NDEBUG
    if (ignore_invalid_ref)
    {
        assert(log != nullptr);
    }
#endif
}

PageEntryMapDeltaBuilder::~PageEntryMapDeltaBuilder() {}

void PageEntryMapDeltaBuilder::apply(PageEntriesEdit & edit)
{
    for (auto && rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT:
        {
            const PageId normal_page_id = base->resolveRefId(rec.page_id);
            v->put(normal_page_id, rec.entry);
            break;
        }
        case WriteBatch::WriteType::DEL:
            v->del<false>(rec.page_id);
            break;
        case WriteBatch::WriteType::REF:
        {
            // Shorten ref-path in case there is RefPage to RefPage
            const PageId normal_page_id = base->resolveRefId(rec.ori_page_id);
            v->ref<false>(rec.page_id, normal_page_id);
            break;
        }
        }
    }
}

void PageEntryMapDeltaBuilder::gcApply(const PageEntriesEdit & edit)
{
    for (const auto & rec : edit.getRecords())
    {
        if (rec.type != WriteBatch::WriteType::PUT)
        {
            continue;
        }
        // Gc only apply PUT for updating page entries
        auto old_iter = base->find(rec.page_id);
        // If the gc page have already been removed, just ignore it
        if (old_iter == base->end())
        {
            continue;
        }
        try
        {
            auto & old_page_entry = old_iter.pageEntry(); // this may throw an exception if ref to non-exist page
            // In case of page being updated during GC process.
            if (old_page_entry.fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                v->put(rec.page_id, rec.entry);
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
        catch (DB::Exception & e)
        {
            // just ignore and continue
        }
    }
}

void PageEntryMapDeltaBuilder::mergeDeltaToBaseInplace( //
    const PageEntryMapDeltaVersionSet::VersionPtr & base,
    const PageEntryMapDeltaVersionSet::VersionPtr & delta)
{
    // apply deletions
    for (auto pid : delta->page_deletions)
        base->del(pid);

    // apply new pages. Note should not auto gen ref
    for (auto && iter : delta->normal_pages)
        base->put(iter.first, iter.second, false);

    // apply new ref
    for (auto && ref_pair : delta->page_ref)
        base->ref(ref_pair.first, ref_pair.second);

    delta->clear();
}

PageEntryMapDeltaVersionSet::VersionPtr
PageEntryMapDeltaBuilder::compactDeltaAndBase(const PageEntryMapDeltaVersionSet::VersionPtr & old_base,
                                              PageEntryMapDeltaVersionSet::VersionPtr &       delta)
{
    PageEntryMapDeltaVersionSet::VersionPtr base = PageEntryMapBase::createBase();
    base->copyEntries(*old_base);
    mergeDeltaToBaseInplace(base, delta);
    return base;
}

PageEntryMapDeltaVersionSet::VersionPtr PageEntryMapDeltaBuilder::compactDeltas( //
    PageEntryMapDeltaVersionSet::BaseType *         vset,
    const PageEntryMapDeltaVersionSet::VersionPtr & tail)
{
    (void)vset;
    if (tail->prev == nullptr || tail->prev->isBase())
    {
        // Only one delta, do nothing
        return nullptr;
    }

    std::stack<PageEntryMapDeltaVersionSet::VersionPtr> nodes;
    auto                                                tmp = PageEntryMapDelta::createDelta();
    for (auto node = tail; node != nullptr; node = node->prev)
    {
        if (node->isBase())
            tmp->prev = node;
        else
            nodes.push(node);
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

bool PageEntryMapDeltaBuilder::needCompactToBase(const PageEntryMapDeltaVersionSet::BaseType *   vset,
                                                 const PageEntryMapDeltaVersionSet::VersionPtr & delta)
{
    assert(!delta->isBase());
    return delta->numDeletions() >= vset->config.compact_hint_delta_deletions //
        || delta->numEntries() >= vset->config.compact_hint_delta_entries;
}

////  PageEntryMapView

const PageEntry & PageEntryMapView::at(const PageId page_id) const
{
    auto iter = this->find(page_id);
    if (likely(iter != end()))
    {
        return iter.pageEntry();
    }
    else
    {
        static PageEntry invalid_entry;
        return invalid_entry;
    }
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

PageEntryMapView::const_iterator PageEntryMapView::find(PageId page_id) const
{
    // begin search PageEntry from tail -> head
    std::shared_ptr<const PageEntryMapDelta> node;
    for (node = tail; node != nullptr; node = node->prev)
    {
        // deleted in later version, then return not exist
        if (node->isDeleted(page_id))
        {
            return this->end();
        }
        auto [is_ref, ori_page_id] = node->isRefId(page_id);
        if (is_ref)
        {
            // if new ref find in this delta, turn to find ori_page_id in this VersionView
            return find(ori_page_id);
        }
        PageEntryMapDelta::const_iterator iter = node->find(page_id);
        if (iter != node->end() || node->isBase())
        {
            return const_iterator(iter);
        }
    }
    assert(false);
    // should not call here
    return const_iterator(std::static_pointer_cast<const PageEntryMapBase>(node)->find(page_id));
}

bool PageEntryMapView::isRefExists(PageId ref_id, PageId page_id) const
{
    auto node = tail;
    for (; node != nullptr; node = node->prev)
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
    return node->isRefExists(ref_id, page_id);
}

PageId PageEntryMapView::resolveRefId(PageId page_id) const
{
    auto node = tail;
    for (; node != nullptr; node = node->prev)
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

PageEntryMapView::const_iterator PageEntryMapView::end() const
{
    return const_iterator(std::static_pointer_cast<const PageEntryMapBase>(vset->current)->end());
}

PageEntryMapBase::const_normal_page_iterator PageEntryMapView::pages_cbegin() const
{
    return vset->current->pages_cbegin();
}

PageEntryMapBase::const_normal_page_iterator PageEntryMapView::pages_cend() const
{
    // FIXME
    return vset->current->pages_cend();
}

PageEntryMapBase::const_iterator PageEntryMapView::cbegin() const
{
    // FIXME

    return vset->current->cbegin();
}

PageEntryMapBase::const_iterator PageEntryMapView::cend() const
{
    // FIXME
    return vset->current->cend();
}

////  PageEntryMapDeltaVersionSet

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::gcApply(const PageEntriesEdit & edit)
{

    std::unique_lock lock(read_mutex);

    // apply edit on base
    PageEntryMapDeltaVersionSet::VersionPtr v;
    {
        auto                     base_view = std::make_shared<PageEntryMapView>(this, current);
        PageEntryMapDeltaBuilder builder(base_view.get());
        builder.gcApply(edit);
        v = builder.build();
    }

    this->appendVersion(std::move(v));

    return listAllLiveFiles();
}

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::listAllLiveFiles() const
{
    std::set<PageFileIdAndLevel> liveFiles;
    for (auto v = current; v != nullptr; v = v->prev)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            liveFiles.insert(it->second.fileIdLevel());
        }
    }
    return liveFiles;
}

} // namespace DB
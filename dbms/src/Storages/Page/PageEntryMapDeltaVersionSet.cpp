#include <Storages/Page/PageEntryMapDeltaVersionSet.h>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

void PageEntryMapDeltaBuilder::apply(const PageEntriesEdit & edit)
{
    (void)base;
    for (const auto & rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT:
            v->put(rec.page_id, rec.entry);
            break;
        case WriteBatch::WriteType::DEL:
            v->del<false>(rec.page_id);
            break;
        case WriteBatch::WriteType::REF:
            v->ref<false>(rec.page_id, rec.ori_page_id);
            break;
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
        auto old_iter = v->find(rec.page_id);
        // If the gc page have already been removed, just ignore it
        if (old_iter == v->end())
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
                old_page_entry = rec.entry;
            }
            // else new page written by another thread, gc page is replaced. leave the page for next gc
        }
        catch (DB::Exception & e)
        {
            // just ignore and continue
        }
    }
}

void PageEntryMapDeltaBuilder::mergeDeltaToBase(PageEntryMap * base, PageEntryMapDelta * delta)
{
    // apply deletions
    for (auto pid : delta->page_deletions)
        base->del(pid);

    // apply new pages
    for (auto && iter : delta->normal_pages)
        base->put(iter.first, iter.second);

    // apply new ref
    for (auto && ref_pair : delta->page_ref)
        base->ref(ref_pair.first, ref_pair.second);

    delta->clear();
}

PageEntryMapDeltaBuilder::PageEntryMapDeltaBuilder(const PageEntryMapView * base_, bool ignore_invalid_ref_, Logger * log_)
    : base(const_cast<PageEntryMapView *>(base_)), v(new PageEntryMapDelta), ignore_invalid_ref(ignore_invalid_ref_), log(log_)
{
#ifndef NDEBUG
    if (ignore_invalid_ref)
    {
        assert(log != nullptr);
    }
#endif
    base->incrRefCount();
}

PageEntryMapDeltaBuilder::~PageEntryMapDeltaBuilder()
{
    base->decrRefCount();
}

void PageEntryMapDeltaBuilder::mergeDeltas(PageEntryMapDeltaVersionSet::BaseType * vset)
{
    auto                             q = vset->current;
    std::vector<PageEntryMapDelta *> nodes_to_remove;
    for (auto p = q->prev; p != &vset->placeholder_node; p = p->prev, q = q->prev)
    {
        if (p->ref_count > 1)
            break;
        // no readers on p, compact q -> p
        p->merge(*q);
        q->clear();
        nodes_to_remove.emplace_back(q);
    }
    // remove unused node from version set
    for (auto node : nodes_to_remove)
        node->decrRefCount();
    // update VersionSet's current node
    vset->current = q;
}


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
    for (const PageEntryMapDelta * node = tail; node != &vset->placeholder_node; node = node->prev)
    {
        max_id = std::max(max_id, node->maxId());
    }
    max_id = std::max(max_id, vset->base->maxId());
    return max_id;
}

PageEntryMapView::const_iterator PageEntryMapView::find(PageId page_id) const
{
    // begin search PageEntry from tail -> head
    for (const PageEntryMapDelta * node = tail; node != &vset->placeholder_node; node = node->prev)
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
        auto iter = node->find(page_id);
        if (iter != node->end())
        {
            return const_iterator(iter);
        }
    }
    return const_iterator(static_cast<const PageEntryMap *>(vset->base)->find(page_id));
}

bool PageEntryMapView::isRefExists(PageId ref_id, PageId page_id) const
{
    for (const PageEntryMapDelta * node = tail; node != &vset->placeholder_node; node = node->prev)
    {
        // `ref_id` or `page_id` has been deleted in later version, then return not exist
        if (node->isDeleted(ref_id) || node->isDeleted(page_id))
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
    return vset->base->isRefExists(ref_id, page_id);
}

PageId PageEntryMapView::resolveRefId(PageId page_id) const
{
    // FIXME
    page_id = vset->base->resolveRefId(page_id);
    return page_id;
}

PageEntryMapView::const_iterator PageEntryMapView::end() const
{
    return const_iterator(static_cast<const PageEntryMap *>(vset->base)->end());
}

PageEntryMap::const_normal_page_iterator PageEntryMapView::pages_cbegin() const
{
    return vset->base->pages_cbegin();
}

PageEntryMap::const_normal_page_iterator PageEntryMapView::pages_cend() const
{
    // FIXME
    return vset->base->pages_cend();
}

PageEntryMap::const_iterator PageEntryMapView::cbegin() const
{
    // FIXME

    return vset->base->cbegin();
}

PageEntryMap::const_iterator PageEntryMapView::cend() const
{
    // FIXME
    return vset->base->cend();
}

} // namespace DB
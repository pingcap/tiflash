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
        collectLiveFilesFromVersionList(s->version()->tail, liveFiles);
    }
    // Iterate over `current`
    collectLiveFilesFromVersionList(current, liveFiles);
    return liveFiles;
}

void PageEntryMapDeltaVersionSet::collectLiveFilesFromVersionList(VersionPtr v, std::set<PageFileIdAndLevel> &liveFiles) const
{
    for (; v != nullptr; v = v->prev)
    {
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            // ignore if it is a tombstone entry
            if (it->second.ref != 0)
            {
                liveFiles.insert(it->second.fileIdLevel());
            }
        }
    }
}

////  PageEntryMapDeltaBuilder

PageEntryMapDeltaBuilder::PageEntryMapDeltaBuilder(const PageEntryMapView * view_, bool ignore_invalid_ref_, Logger * log_)
    : view(const_cast<PageEntryMapView *>(view_)),
      v(view->tail),
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
            this->applyPut(rec);
            break;
        case WriteBatch::WriteType::DEL:
            this->applyDel(rec);
            break;
        case WriteBatch::WriteType::REF:
            this->applyRef(rec);
            break;
        }
    }
}

void PageEntryMapDeltaBuilder::applyPut(PageEntriesEdit::EditRecord &rec)
{
    assert(rec.type == WriteBatch::WriteType::PUT);
    v->ref_deletions.erase(rec.page_id);
    const PageId normal_page_id = view->resolveRefId(rec.page_id);

    // update ref-pairs
    bool is_ref_exist = view->isRefExists(rec.page_id, normal_page_id);
    if (!is_ref_exist)
    {
        v->page_ref.emplace(rec.page_id, normal_page_id);
    }

    // update normal page's entry
    auto old_entry = view->findNormalPageEntry(normal_page_id);
    assert(!is_ref_exist || (is_ref_exist && old_entry != nullptr));
    if (old_entry == nullptr)
    {
        // Page{normal_page_id} not exist
        rec.entry.ref = 1;
        v->normal_pages[normal_page_id] = rec.entry;
    }
    else
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        rec.entry.ref = old_entry->ref + !is_ref_exist;
        v->normal_pages[normal_page_id] = rec.entry;
    }

    v->max_page_id = std::max(v->max_page_id, rec.page_id);
}

void PageEntryMapDeltaBuilder::applyDel(PageEntriesEdit::EditRecord &rec)
{
    assert(rec.type == WriteBatch::WriteType::DEL);
    const PageId normal_page_id = view->resolveRefId(rec.page_id);
    v->ref_deletions.insert(rec.page_id);
    v->page_ref.erase(rec.page_id);
    this->decreasePageRef(normal_page_id);
}

void PageEntryMapDeltaBuilder::applyRef(PageEntriesEdit::EditRecord &rec)
{
    assert(rec.type == WriteBatch::WriteType::REF);
    v->ref_deletions.erase(rec.page_id);
    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = view->resolveRefId(rec.ori_page_id);
    auto old_entry = view->findNormalPageEntry(normal_page_id);
    if (likely(old_entry != nullptr))
    {
        // if RefPage{ref_id} already exist, release that ref first
        auto [is_ref_id, old_normal_id] = view->isRefId(rec.page_id);
        if (unlikely(is_ref_id))
        {
            // if RefPage{ref-id} -> Page{normal_page_id} already exists, just ignore
            if (old_normal_id == normal_page_id)
                return;
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
        // The Page to be ref is not exist.
        if (ignore_invalid_ref)
        {
            LOG_WARNING(log, "Ignore invalid RefPage while opening PageStorage: RefPage" + DB::toString(rec.page_id) + " to non-exist Page" + DB::toString(rec.ori_page_id));
        }
        else
        {
            // accept dangling ref if we are writing to a tmp entry map.
            // like entry map of WriteBatch or Gc or AnalyzeMeta
            v->page_ref[rec.page_id] = normal_page_id;
        }
    }
    v->max_page_id = std::max(v->max_page_id, rec.page_id);
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
            continue;
        // Gc only apply PUT for updating page entries
        try
        {
            auto old_page_entry = view->find(rec.page_id); // this may throw an exception if ref to non-exist page
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

void PageEntryMapDeltaBuilder::gcApplyInplace(const PageEntryMapDeltaVersionSet::VersionPtr & current, PageEntriesEdit & edit)
{
    for (auto & rec : edit.getRecords())
    {
        if (rec.type != WriteBatch::WriteType::PUT)
            continue;
        // Gc only apply PUT for updating page entries
        try
        {
            auto old_page_entry = current->find(rec.page_id); // this may throw an exception if ref to non-exist page
            // If the gc page have already been removed, just ignore it
            if (old_page_entry == nullptr)
                continue;
            // In case of page being updated during GC process.
            if (old_page_entry->fileIdLevel() < rec.entry.fileIdLevel())
            {
                // no new page write to `page_entry_map`, replace it with gc page
                rec.entry.ref = old_page_entry->ref;
                current->normal_pages[rec.page_id] = rec.entry;
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
    auto old_entry = view->findNormalPageEntry(page_id);
    if (old_entry != nullptr)
    {
        auto entry = *old_entry;
        entry.ref = old_entry->ref <= 1? 0: old_entry->ref - 1;
        // keep an entry of ref-count == 0, so that we can delete this entry when merged to base
        v->normal_pages[page_id] = entry;
    }
}


} // namespace DB
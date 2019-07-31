#include <Storages/Page/PageEntryMapDeltaVersionSet.h>

#include <stack>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{

//==========================================================================================
// PageEntryMapDeltaVersionSet
//==========================================================================================

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::gcApply(PageEntriesEdit & edit)
{
    std::unique_lock lock(read_mutex);

    if (current.use_count() == 1 && current->isBase())
    {
        // If no readers, we could directly merge edits
        EditAcceptor::gcApplyInplace(current, edit);
    }
    else
    {
        if (current.use_count() != 1)
        {
            VersionPtr v = VersionType::createDelta();
            appendVersion(std::move(v));
        }
        auto         view = std::make_shared<PageEntryMapView>(current);
        EditAcceptor builder(view.get());
        builder.gcApply(edit);
    }

    return listAllLiveFiles();
}

std::set<PageFileIdAndLevel> PageEntryMapDeltaVersionSet::listAllLiveFiles() const
{
    // Note read_mutex must be hold.
    std::set<PageFileIdAndLevel> liveFiles;
    std::set<VersionPtr>         visitedVersions; // avoid to access same version multiple time
    // Iterate all snapshot to collect all PageFile in used.
    for (auto s = snapshots->next; s != snapshots.get(); s = s->next)
    {
        collectLiveFilesFromVersionList(s->version()->getSharedTailVersion(), visitedVersions, liveFiles);
    }
    // Iterate over `current`
    collectLiveFilesFromVersionList(current, visitedVersions, liveFiles);
    return liveFiles;
}

void PageEntryMapDeltaVersionSet::collectLiveFilesFromVersionList( //
    VersionPtr                     v,
    std::set<VersionPtr> &         visited,
    std::set<PageFileIdAndLevel> & liveFiles) const
{
    for (; v != nullptr; v = v->prev)
    {
        // If this version has been visited, all previous version has been collected.
        if (visited.count(v) > 0)
            break;
        for (auto it = v->pages_cbegin(); it != v->pages_cend(); ++it)
        {
            // ignore if it is a tombstone entry
            if (it->second.ref != 0)
            {
                liveFiles.insert(it->second.fileIdLevel());
            }
        }
        visited.insert(v);
    }
}

//==========================================================================================
// Functions used when view release and do compact on version-list
//==========================================================================================

PageEntryMapDeltaVersionSet::VersionPtr           //
PageEntryMapDeltaVersionSet::compactDeltaAndBase( //
    const PageEntryMapDeltaVersionSet::VersionPtr & old_base,
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

PageEntryMapDeltaVersionSet::VersionPtr //
PageEntryMapDeltaVersionSet::compactDeltas(const PageEntryMapDeltaVersionSet::VersionPtr & tail)
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

//==========================================================================================
// DeltaVersionEditAcceptor
//==========================================================================================

DeltaVersionEditAcceptor::DeltaVersionEditAcceptor(const PageEntryMapView * view_, bool ignore_invalid_ref_, Logger * log_)
    : view(const_cast<PageEntryMapView *>(view_)),
      current_version(view->getSharedTailVersion()),
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

DeltaVersionEditAcceptor::~DeltaVersionEditAcceptor() = default;

/// Apply edits and generate new delta
void DeltaVersionEditAcceptor::apply(PageEntriesEdit & edit)
{
    assert(!current_version->isBase());
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

void DeltaVersionEditAcceptor::applyPut(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::PUT);
    current_version->ref_deletions.erase(rec.page_id);

    auto [is_ref_exist, normal_page_id] = view->isRefId(rec.page_id);
    if (!is_ref_exist)
    {
        // if ref not exist, add new ref-pair
        normal_page_id = rec.page_id;
        current_version->page_ref.emplace(rec.page_id, normal_page_id);
    }

    // update normal page's entry
    auto old_entry = view->findNormalPageEntry(normal_page_id);
    assert(!is_ref_exist || (is_ref_exist && old_entry != nullptr));
    if (old_entry == nullptr)
    {
        // Page{normal_page_id} not exist
        rec.entry.ref                                 = 1;
        current_version->normal_pages[normal_page_id] = rec.entry;
    }
    else
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        rec.entry.ref                                 = old_entry->ref + !is_ref_exist;
        current_version->normal_pages[normal_page_id] = rec.entry;
    }

    current_version->max_page_id = std::max(current_version->max_page_id, rec.page_id);
}

void DeltaVersionEditAcceptor::applyDel(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::DEL);
    const PageId normal_page_id = view->resolveRefId(rec.page_id);
    current_version->ref_deletions.insert(rec.page_id);
    current_version->page_ref.erase(rec.page_id);
    this->decreasePageRef(normal_page_id);
}

void DeltaVersionEditAcceptor::applyRef(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::REF);
    current_version->ref_deletions.erase(rec.page_id);
    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = view->resolveRefId(rec.ori_page_id);
    auto         old_entry      = view->findNormalPageEntry(normal_page_id);
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
        current_version->page_ref[rec.page_id] = normal_page_id;
        // increase entry's ref-count
        auto new_entry = *old_entry;
        new_entry.ref += 1;
        current_version->normal_pages[rec.page_id] = new_entry;
    }
    else
    {
        // The Page to be ref is not exist.
        if (ignore_invalid_ref)
        {
            LOG_WARNING(log,
                        "Ignore invalid RefPage while opening PageStorage: RefPage" + DB::toString(rec.page_id) + " to non-exist Page"
                            + DB::toString(rec.ori_page_id));
        }
        else
        {
            // accept dangling ref if we are writing to a tmp entry map.
            // like entry map of WriteBatch or Gc or AnalyzeMeta
            current_version->page_ref[rec.page_id] = normal_page_id;
        }
    }
    current_version->max_page_id = std::max(current_version->max_page_id, rec.page_id);
}

void DeltaVersionEditAcceptor::applyInplace(const PageEntryMapDeltaVersionSet::VersionPtr & current, const PageEntriesEdit & edit)
{
    assert(current->isBase());
    assert(current.use_count() == 1);
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

void DeltaVersionEditAcceptor::decreasePageRef(const PageId page_id)
{
    auto old_entry = view->findNormalPageEntry(page_id);
    if (old_entry != nullptr)
    {
        auto entry = *old_entry;
        entry.ref  = old_entry->ref <= 1 ? 0 : old_entry->ref - 1;
        // Keep an tombstone entry (ref-count == 0), so that we can delete this entry when merged to base
        current_version->normal_pages[page_id] = entry;
    }
}

} // namespace DB
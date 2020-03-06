#include <Storages/Page/stable/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/stable/VersionSet/PageEntriesVersionSetWithDelta.h>

#include <stack>

namespace DB::stable
{

//==========================================================================================
// PageEntriesVersionSetWithDelta
//==========================================================================================

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> //
PageEntriesVersionSetWithDelta::gcApply(PageEntriesEdit & edit, bool need_scan_page_ids)
{
    std::unique_lock lock(read_write_mutex);
    if (!edit.empty())
    {
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
                appendVersion(std::move(v), lock);
            }
            auto         view = std::make_shared<PageEntriesView>(current);
            EditAcceptor builder(view.get());
            builder.gcApply(edit);
        }
    }
    return listAllLiveFiles(lock, need_scan_page_ids);
}

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>>
PageEntriesVersionSetWithDelta::listAllLiveFiles(const std::unique_lock<std::shared_mutex> & lock, bool need_scan_page_ids) const
{
    (void)lock; // Note read_write_mutex must be hold.

    /// TODO: this is costly, maybe we should find a better way not to block generating other snapshot.
    std::set<PageFileIdAndLevel> live_files;
    std::set<PageId>             live_normal_pages;
    // Iterate all snapshot to collect all PageFile in used.
    for (auto s = snapshots->next; s != snapshots.get(); s = s->next)
    {
        collectLiveFilesFromVersionList(*(s->version()), live_files, live_normal_pages, need_scan_page_ids);
    }
    // Iterate over `current`
    PageEntriesView latest_view(current);
    collectLiveFilesFromVersionList(latest_view, live_files, live_normal_pages, need_scan_page_ids);
    return {live_files, live_normal_pages};
}

void PageEntriesVersionSetWithDelta::collectLiveFilesFromVersionList( //
    const PageEntriesView &        view,
    std::set<PageFileIdAndLevel> & live_files,
    std::set<PageId> &             live_normal_pages,
    bool                           need_scan_page_ids) const
{
    std::set<PageId> normal_pages_this_snapshot = view.validNormalPageIds();
    for (auto normal_page_id : normal_pages_this_snapshot)
    {
        if (auto entry = view.findNormalPageEntry(normal_page_id); entry && !entry->isTombstone())
        {
            if (need_scan_page_ids)
                live_normal_pages.insert(normal_page_id);
            live_files.insert(entry->fileIdLevel());
        }
    }
}
//==========================================================================================
// DeltaVersionEditAcceptor
//==========================================================================================

DeltaVersionEditAcceptor::DeltaVersionEditAcceptor(const PageEntriesView * view_, bool ignore_invalid_ref_, Logger * log_)
    : view(const_cast<PageEntriesView *>(view_)),
      current_version(view->getSharedTailVersion()),
      ignore_invalid_ref(ignore_invalid_ref_),
      log(log_)
{
#ifndef NDEBUG
    // tail of view must be a delta
    assert(!current_version->isBase());
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
        case WriteBatch::WriteType::UPSERT:
            throw Exception("WriteType::MOVE_NORMAL_PAGE should only write by gcApply!", ErrorCodes::LOGICAL_ERROR);
            break;
        }
    }
}

void DeltaVersionEditAcceptor::applyPut(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::PUT);
    /// Note that any changes on `current_version` will break the consistency of `view`.
    /// We should postpone changes to the last of this function.

    auto [is_ref_exist, normal_page_id] = view->isRefId(rec.page_id);
    if (!is_ref_exist)
    {
        // if ref not exist, we should add new ref-pair later
        normal_page_id = rec.page_id;
    }

    // update normal page's entry
    const auto old_entry = view->findNormalPageEntry(normal_page_id);
    if (is_ref_exist && !old_entry)
    {
        throw DB::Exception("Accessing RefPage" + DB::toString(rec.page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                            ErrorCodes::LOGICAL_ERROR);
    }

    if (!old_entry)
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

    // Add new ref-pair if not exists.
    if (!is_ref_exist)
        current_version->page_ref.emplace(rec.page_id, normal_page_id);
    current_version->ref_deletions.erase(rec.page_id);
    current_version->max_page_id = std::max(current_version->max_page_id, rec.page_id);
}

void DeltaVersionEditAcceptor::applyDel(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::DEL);
    /// Note that any changes on `current_version` will break the consistency of `view`.
    /// We should postpone changes to the last of this function.

    auto [is_ref, normal_page_id] = view->isRefId(rec.page_id);
    current_version->ref_deletions.insert(rec.page_id);
    current_version->page_ref.erase(rec.page_id);
    if (is_ref)
    {
        // If ref exists, we need to decrease entry ref-count
        decreasePageRef(normal_page_id);
    }
}

void DeltaVersionEditAcceptor::applyRef(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatch::WriteType::REF);
    /// Note that any changes on `current_version` will break the consistency of `view`.
    /// We should postpone changes to the last of this function.

    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = view->resolveRefId(rec.ori_page_id);
    const auto   old_entry      = view->findNormalPageEntry(normal_page_id);
    if (likely(old_entry))
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
        current_version->normal_pages[normal_page_id] = new_entry;

        current_version->ref_deletions.erase(rec.page_id);
        current_version->max_page_id = std::max(current_version->max_page_id, rec.page_id);
    }
    else
    {
        // The Page to be ref is not exist.
        if (ignore_invalid_ref)
        {
            LOG_WARNING(log,
                        "Ignore invalid RefPage in DeltaVersionEditAcceptor::applyRef, RefPage" + DB::toString(rec.page_id)
                            + " to non-exist Page" + DB::toString(rec.ori_page_id));
        }
        else
        {
            throw Exception("Try to add RefPage" + DB::toString(rec.page_id) + " to non-exist Page" + DB::toString(rec.ori_page_id),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void DeltaVersionEditAcceptor::applyInplace(const PageEntriesVersionSetWithDelta::VersionPtr & current,
                                            const PageEntriesEdit &                            edit,
                                            Poco::Logger *                                     log)
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
            try
            {
                current->ref(rec.page_id, rec.ori_page_id);
            }
            catch (DB::Exception & e)
            {
                LOG_WARNING(log,
                            "Ignore invalid RefPage in DeltaVersionEditAcceptor::applyInplace, RefPage" + DB::toString(rec.page_id)
                                + " to non-exist Page" + DB::toString(rec.ori_page_id));
            }
            break;
        case WriteBatch::WriteType::UPSERT:
            current->upsertPage(rec.page_id, rec.entry);
            break;
        }
    }
}

void DeltaVersionEditAcceptor::decreasePageRef(const PageId page_id)
{
    const auto old_entry = view->findNormalPageEntry(page_id);
    if (old_entry)
    {
        auto entry = *old_entry;
        entry.ref  = old_entry->ref <= 1 ? 0 : old_entry->ref - 1;
        // Keep an tombstone entry (ref-count == 0), so that we can delete this entry when merged to base
        current_version->normal_pages[page_id] = entry;
    }
}

} // namespace DB::stable

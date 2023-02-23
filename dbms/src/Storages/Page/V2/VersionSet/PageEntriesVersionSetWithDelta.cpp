// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <stack>

#ifdef FIU_ENABLE
#include <Common/randomSeed.h>

#include <pcg_random.hpp>
#include <thread>
#endif

namespace ProfileEvents
{
extern const Event PSMVCCCompactOnDelta;
extern const Event PSMVCCCompactOnDeltaRebaseRejected;
extern const Event PSMVCCCompactOnBase;
extern const Event PSMVCCCompactOnBaseCommit;
extern const Event PSMVCCApplyOnCurrentBase;
extern const Event PSMVCCApplyOnCurrentDelta;
extern const Event PSMVCCApplyOnNewDelta;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric PSMVCCSnapshotsList;
} // namespace CurrentMetrics

namespace DB
{
namespace FailPoints
{
extern const char random_slow_page_storage_list_all_live_files[];
extern const char random_slow_page_storage_remove_expired_snapshots[];
} // namespace FailPoints

namespace PS::V2
{
//==========================================================================================
// PageEntriesVersionSetWithDelta
//==========================================================================================

void PageEntriesVersionSetWithDelta::apply(PageEntriesEdit & edit)
{
    std::unique_lock read_lock(read_write_mutex);

    if (current.use_count() == 1 && current->isBase())
    {
        ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnCurrentBase);
        // If no readers, we could directly merge edits.
        DeltaVersionEditAcceptor::applyInplace(name, current, edit, log);
        return;
    }

    if (current.use_count() != 1)
    {
        ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnNewDelta);
        // There are reader(s) on current, generate new delta version and append to version-list
        VersionPtr v = PageEntriesForDelta::createDelta();
        appendVersion(std::move(v), read_lock);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnCurrentDelta);
    }
    // Make a view from head to new version, then apply edits on `current`.
    auto view = std::make_shared<PageEntriesView>(current);
    EditAcceptor builder(view.get(), name, /* ignore_invalid_ref_= */ true, log);
    builder.apply(edit);
}

size_t PageEntriesVersionSetWithDelta::size() const
{
    std::shared_lock read_lock(read_write_mutex);
    return sizeUnlocked();
}

size_t PageEntriesVersionSetWithDelta::sizeUnlocked() const
{
    size_t sz = 0;
    for (auto v = current; v != nullptr; v = std::atomic_load(&v->prev))
    {
        sz += 1;
    }
    return sz;
}

SnapshotsStatistics PageEntriesVersionSetWithDelta::getSnapshotsStat() const
{
    // Note: this will scan and remove expired weak_ptrs from `snapshots`
    return removeExpiredSnapshots();
}


PageEntriesVersionSetWithDelta::SnapshotPtr PageEntriesVersionSetWithDelta::getSnapshot(const String & tracing_id, BackgroundProcessingPool::TaskHandle handle)
{
    // acquire for unique_lock since we need to add all snapshots to link list
    std::unique_lock<std::shared_mutex> lock(read_write_mutex);

    auto s = std::make_shared<Snapshot>(this, current, tracing_id, handle);
    // Register a weak_ptr to snapshot into VersionSet so that we can get all living PageFiles
    // by `PageEntriesVersionSetWithDelta::listAllLiveFiles`, and it remove useless weak_ptr of snapshots.
    // Do not call `vset->removeExpiredSnapshots` inside `~Snapshot`, or it may cause incursive deadlock
    // on `vset->read_write_mutex`.
    snapshots.emplace_back(SnapshotWeakPtr(s));
    CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
    return s;
}

void PageEntriesVersionSetWithDelta::appendVersion(VersionPtr && v, const std::unique_lock<std::shared_mutex> & lock)
{
    (void)lock; // just for ensure lock is hold
    assert(v != current);
    // Append to linked list
    v->prev = current;
    current = v;
}

PageEntriesVersionSetWithDelta::RebaseResult PageEntriesVersionSetWithDelta::rebase(const VersionPtr & old_base, const VersionPtr & new_base)
{
    assert(old_base != nullptr);
    std::unique_lock lock(read_write_mutex);
    // Should check `old_base` is valid
    if (!isValidVersion(old_base))
    {
        return RebaseResult::INVALID_VERSION;
    }
    if (old_base == current)
    {
        current = new_base;
        return RebaseResult::SUCCESS;
    }

    auto q = current, p = std::atomic_load(&current->prev);
    while (p != nullptr && p != old_base)
    {
        q = p;
        p = std::atomic_load(&q->prev);
    }
    // p must point to `old_base` now
    assert(p == old_base);
    // rebase q on `new_base`
    std::atomic_store(&q->prev, new_base);
    return RebaseResult::SUCCESS;
}

std::unique_lock<std::shared_mutex> PageEntriesVersionSetWithDelta::acquireForLock()
{
    return std::unique_lock<std::shared_mutex>(read_write_mutex);
}

bool PageEntriesVersionSetWithDelta::isValidVersion(VersionPtr tail) const
{
    for (auto node = current; node != nullptr; node = std::atomic_load(&node->prev))
    {
        if (node == tail)
        {
            return true;
        }
    }
    return false;
}

void PageEntriesVersionSetWithDelta::compactUntil(VersionPtr tail)
{
    if (tail == nullptr || tail->isBase())
        return;

    {
        // If we can not found tail from `current` version-list, then other view has already
        // do compaction on `tail` version, and we can just free that version
        std::shared_lock lock(read_write_mutex);
        if (!isValidVersion(tail))
            return;
    }
    // do compact on delta
    ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDelta);
    VersionPtr tmp = PageEntriesForDelta::compactDeltas(tail); // Note: May be compacted by different threads
    if (tmp != nullptr)
    {
        // rebase vset->current on `this->tail` to base on `tmp`
        if (this->rebase(tail, tmp) == RebaseResult::INVALID_VERSION)
        {
            // Another thread may have done compaction and rebase, then we just release `tail`
            ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDeltaRebaseRejected);
            return;
        }
        // release tail ref on this view, replace with tmp
        tail = tmp;
        tmp.reset();
    }

    if (!tail->shouldCompactToBase(config))
    {
        return;
    }

    // do compact on base
    ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnBase);
    auto old_base = std::atomic_load(&tail->prev);
    assert(old_base != nullptr);
    // create a new_base and copy the entries from `old_base` and `tail`
    VersionPtr new_base = PageEntriesForDelta::compactDeltaAndBase(old_base, tail);
    // replace nodes [head, tail] by new_base
    if (this->rebase(tail, new_base) == RebaseResult::INVALID_VERSION)
    {
        // Another thread may have done compaction and rebase, then we just release `tail`. In case we may add more code after do compaction on base
        ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDeltaRebaseRejected);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnBaseCommit);
    }
}

SnapshotsStatistics PageEntriesVersionSetWithDelta::removeExpiredSnapshots() const
{
    // Notice: we should free those valid snapshots without locking, or it may cause
    // incursive deadlock on `vset->read_write_mutex`.
    std::vector<SnapshotPtr> valid_snapshots;
    SnapshotsStatistics stats;
    DB::Int64 num_snapshots_removed = 0;
    {
        std::unique_lock lock(read_write_mutex);
        for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
        {
            auto snapshot_or_invalid = iter->lock();
            if (snapshot_or_invalid == nullptr)
            {
                // Clear expired snapshots weak_ptrs
                iter = snapshots.erase(iter);
                num_snapshots_removed += 1;
            }
            else
            {
                fiu_do_on(FailPoints::random_slow_page_storage_remove_expired_snapshots, {
                    pcg64 rng(randomSeed());
                    std::chrono::milliseconds ms{std::uniform_int_distribution(0, 900)(rng)}; // 0~900 milliseconds
                    std::this_thread::sleep_for(ms);
                });
                const auto snapshot_lifetime = snapshot_or_invalid->elapsedSeconds();
                if (snapshot_lifetime > stats.longest_living_seconds)
                {
                    stats.longest_living_seconds = snapshot_lifetime;
                    stats.longest_living_from_thread_id = snapshot_or_invalid->create_thread;
                    stats.longest_living_from_tracing_id = snapshot_or_invalid->tracing_id;
                }
                valid_snapshots.emplace_back(snapshot_or_invalid); // Save valid snapshot and release them without lock later
                iter++;
            }
        }
    } // unlock `read_write_mutex`

    stats.num_snapshots = valid_snapshots.size();
    valid_snapshots.clear();

    CurrentMetrics::sub(CurrentMetrics::PSMVCCSnapshotsList, num_snapshots_removed);
    // Return some statistics of the oldest living snapshot.
    return stats;
}


std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> //
PageEntriesVersionSetWithDelta::gcApply( //
    PageEntriesEdit & edit,
    bool need_scan_page_ids)
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
            auto view = std::make_shared<PageEntriesView>(current);
            EditAcceptor builder(view.get(), name);
            builder.gcApply(edit);
        }
    }
    return listAllLiveFiles(std::move(lock), need_scan_page_ids);
}

std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>>
PageEntriesVersionSetWithDelta::listAllLiveFiles(std::unique_lock<std::shared_mutex> && lock, bool need_scan_page_ids)
{
    constexpr const double exist_stale_snapshot = 60.0;

    /// Collect live files is costly, we save SnapshotPtrs and scan them without lock.
    // Note read_write_mutex must be hold.
    std::vector<SnapshotPtr> valid_snapshots;
    const size_t snapshots_size_before_clean = snapshots.size();
    SnapshotsStatistics stats;
    for (auto iter = snapshots.begin(); iter != snapshots.end(); /* empty */)
    {
        auto snapshot_or_invalid = iter->lock();
        if (snapshot_or_invalid == nullptr)
        {
            // Clear free snapshots
            iter = snapshots.erase(iter);
        }
        else
        {
            fiu_do_on(DB::FailPoints::random_slow_page_storage_list_all_live_files, {
                pcg64 rng(randomSeed());
                std::chrono::milliseconds ms{std::uniform_int_distribution(0, 900)(rng)}; // 0~900 milliseconds
                std::this_thread::sleep_for(ms);
            });
            const auto snapshot_lifetime = snapshot_or_invalid->elapsedSeconds();
            if (snapshot_lifetime > stats.longest_living_seconds)
            {
                stats.longest_living_seconds = snapshot_lifetime;
                stats.longest_living_from_thread_id = snapshot_or_invalid->create_thread;
                stats.longest_living_from_tracing_id = snapshot_or_invalid->tracing_id;
            }
            if (snapshot_lifetime > exist_stale_snapshot)
            {
                LOG_WARNING(
                    log,
                    "Suspicious stale snapshot detected lifetime {:.3f} seconds, created from thread_id {}, tracing_id {}",
                    snapshot_lifetime,
                    snapshot_or_invalid->create_thread,
                    snapshot_or_invalid->tracing_id);
            }
            valid_snapshots.emplace_back(snapshot_or_invalid); // Save valid snapshot and release them without lock later
            iter++;
        }
    }
    // Create a temporary latest snapshot by using `current`
    // release this temporary snapshot won't cause version-list compact
    valid_snapshots.emplace_back(std::make_shared<Snapshot>(this, current, "", nullptr));

    lock.unlock(); // Notice: unlock and we should free those valid snapshots without locking

    stats.num_snapshots = valid_snapshots.size();
    // Plus 1 for eliminating the counting of temporary snapshot of `current`
    const size_t num_invalid_snapshot_to_clean = snapshots_size_before_clean + 1 - valid_snapshots.size();
    if (num_invalid_snapshot_to_clean > 0)
    {
        CurrentMetrics::sub(CurrentMetrics::PSMVCCSnapshotsList, num_invalid_snapshot_to_clean);
#define STALE_SNAPSHOT_LOG_PARAMS                          \
    "{} gcApply remove {} invalid snapshots, "             \
    "{} snapshots left, longest lifetime {:.3f} seconds, " \
    "created from thread_id {}, tracing_id {}",            \
        name,                                              \
        num_invalid_snapshot_to_clean,                     \
        stats.num_snapshots,                               \
        stats.longest_living_seconds,                      \
        stats.longest_living_from_thread_id,               \
        stats.longest_living_from_tracing_id
        if (stats.longest_living_seconds > exist_stale_snapshot)
            LOG_WARNING(log, STALE_SNAPSHOT_LOG_PARAMS);
        else
            LOG_DEBUG(log, STALE_SNAPSHOT_LOG_PARAMS);
    }
    // Iterate all snapshots to collect all PageFile in used.
    std::set<PageFileIdAndLevel> live_files;
    std::set<PageId> live_normal_pages;
    for (const auto & snap : valid_snapshots)
    {
        if (unlikely(snap == nullptr))
        {
            LOG_WARNING(log, "{} gcApply get invalid snapshot for collectLiveFilesFromVersionList, ignored.", name);
            continue;
        }
        collectLiveFilesFromVersionList(*snap->version(), live_files, live_normal_pages, need_scan_page_ids);
    }
    return {live_files, live_normal_pages};
}

void PageEntriesVersionSetWithDelta::collectLiveFilesFromVersionList( //
    const PageEntriesView & view,
    std::set<PageFileIdAndLevel> & live_files,
    std::set<PageId> & live_normal_pages,
    bool need_scan_page_ids)
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

DeltaVersionEditAcceptor::DeltaVersionEditAcceptor(const PageEntriesView * view_,
                                                   const String & name_,
                                                   bool ignore_invalid_ref_,
                                                   Poco::Logger * log_)
    : view(const_cast<PageEntriesView *>(view_))
    , current_version(view->getSharedTailVersion())
    , ignore_invalid_ref(ignore_invalid_ref_)
    , name(name_)
    , log(log_)
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
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT:
            this->applyPut(rec);
            break;
        case WriteBatchWriteType::DEL:
            this->applyDel(rec);
            break;
        case WriteBatchWriteType::REF:
            this->applyRef(rec);
            break;
        case WriteBatchWriteType::UPSERT:
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("WriteType::UPSERT should only write by gcApply!", ErrorCodes::LOGICAL_ERROR);
            break;
        }
    }
}

void DeltaVersionEditAcceptor::applyPut(PageEntriesEdit::EditRecord & rec)
{
    assert(rec.type == WriteBatchWriteType::PUT);
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
        rec.entry.ref = 1;
        current_version->normal_pages[normal_page_id] = rec.entry;
    }
    else
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        rec.entry.ref = old_entry->ref + !is_ref_exist;
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
    assert(rec.type == WriteBatchWriteType::DEL);
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
    assert(rec.type == WriteBatchWriteType::REF);
    /// Note that any changes on `current_version` will break the consistency of `view`.
    /// We should postpone changes to the last of this function.

    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = view->resolveRefId(rec.ori_page_id);
    const auto old_entry = view->findNormalPageEntry(normal_page_id);
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
            LOG_WARNING(log, "{} Ignore invalid RefPage in DeltaVersionEditAcceptor::applyRef, RefPage{} to non-exist Page{}", name, rec.page_id, rec.ori_page_id);
        }
        else
        {
            throw Exception("Try to add RefPage" + DB::toString(rec.page_id) + " to non-exist Page" + DB::toString(rec.ori_page_id),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void DeltaVersionEditAcceptor::applyInplace(const String & name,
                                            const PageEntriesVersionSetWithDelta::VersionPtr & current,
                                            const PageEntriesEdit & edit,
                                            Poco::Logger * log)
{
    assert(current->isBase());
    assert(current.use_count() == 1);
    for (auto && rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT:
            current->put(rec.page_id, rec.entry);
            break;
        case WriteBatchWriteType::DEL:
            current->del(rec.page_id);
            break;
        case WriteBatchWriteType::REF:
            // Shorten ref-path in case there is RefPage to RefPage
            try
            {
                current->ref(rec.page_id, rec.ori_page_id);
            }
            catch (DB::Exception & e)
            {
                LOG_WARNING(log, "{} Ignore invalid RefPage in DeltaVersionEditAcceptor::applyInplace, RefPage{} to non-exist Page{}", name, rec.page_id, rec.ori_page_id);
            }
            break;
        case WriteBatchWriteType::UPSERT:
            current->upsertPage(rec.page_id, rec.entry);
            break;
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void DeltaVersionEditAcceptor::decreasePageRef(const PageId page_id)
{
    const auto old_entry = view->findNormalPageEntry(page_id);
    if (old_entry)
    {
        auto entry = *old_entry;
        entry.ref = old_entry->ref <= 1 ? 0 : old_entry->ref - 1;
        // Keep an tombstone entry (ref-count == 0), so that we can delete this entry when merged to base
        current_version->normal_pages[page_id] = entry;
    }
}

} // namespace PS::V2
} // namespace DB

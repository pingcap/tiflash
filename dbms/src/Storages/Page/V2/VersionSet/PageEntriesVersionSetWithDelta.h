// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V2/PageDefines.h>
#include <Storages/Page/V2/PageEntries.h>
#include <Storages/Page/V2/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/V2/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/V2/VersionSet/PageEntriesView.h>

#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <utility>

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
} // namespace CurrentMetrics

namespace DB::PS::V2
{
class DeltaVersionEditAcceptor;

class PageEntriesVersionSetWithDelta
{
public:
    using EditAcceptor = DeltaVersionEditAcceptor;
    using VersionType = PageEntriesForDelta;
    using VersionPtr = std::shared_ptr<VersionType>;

public:
    explicit PageEntriesVersionSetWithDelta(String name_, const MVCC::VersionSetConfig & config_, LoggerPtr log_)
        : current(VersionType::createBase())
        , config(config_)
        , name(std::move(name_))
        , log(log_)
    {}

    ~PageEntriesVersionSetWithDelta()
    {
        current.reset();

        removeExpiredSnapshots();

        // snapshot list is empty
        assert(snapshots.empty());
    }

    void apply(PageEntriesEdit & edit);

    size_t size() const;

    size_t sizeUnlocked() const;

    bool tryCompact()
    {
        const auto release_idx = last_released_snapshot_index.load();
        const auto last_try_idx = last_try_compact_index.load();
        if (release_idx <= last_try_idx)
        {
            return false;
        }

        // some new snapshot are released, let's try
        // compact the versions.
        last_try_compact_index.store(release_idx);

        // compact version list with the latest snapshot.
        // do NOT increase the index by this snapshot or it will
        // cause inf loop
        auto snap = getSnapshot("ps-mem-compact", nullptr);
        compactUntil(snap->view.getSharedTailVersion());

        // try compact again
        return true;
    }

    SnapshotsStatistics getSnapshotsStat() const;

    std::string toDebugString() const
    {
        std::shared_lock lock(read_write_mutex);
        return versionToDebugString(current);
    }

    static std::string versionToDebugString(VersionPtr tail)
    {
        std::string s;
        bool is_first = true;
        std::stack<VersionPtr> deltas;
        for (auto v = tail; v != nullptr; v = std::atomic_load(&v->prev))
        {
            deltas.emplace(v);
        }
        while (!deltas.empty())
        {
            auto v = deltas.top();
            deltas.pop();
            s += is_first ? "" : "<-";
            is_first = false;
            s += "{\"rc\":";
            s += DB::toString(v.use_count() - 1);
            s += ",\"addr\":", s += DB::ptrToString(v.get());
            s += '}';
        }
        return s;
    }

    /// Snapshot.
    /// When snapshot object is freed, it will call `view.release()` to compact VersionList,
    /// and its weak_ptr in the VersionSet's snapshots list will become empty.
    class Snapshot
        : public DB::PageStorageSnapshot
        , private boost::noncopyable
    {
    public:
        PageEntriesVersionSetWithDelta * vset;
        PageEntriesView view;

        using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
        const unsigned create_thread;
        const String tracing_id;

    private:
        const TimePoint create_time;

        // it should be a weak_ptr because the handle may be released before snapshot released
        std::weak_ptr<BackgroundProcessingPool::TaskInfo> compact_handle;

    public:
        Snapshot(
            PageEntriesVersionSetWithDelta * vset_,
            VersionPtr tail_,
            const String & tracing_id_,
            BackgroundProcessingPool::TaskHandle handle)
            : vset(vset_)
            , view(std::move(tail_))
            , create_thread(Poco::ThreadNumber::get())
            , tracing_id(tracing_id_)
            , create_time(std::chrono::steady_clock::now())
            , compact_handle(handle)
        {
            CurrentMetrics::add(CurrentMetrics::PSMVCCNumSnapshots);
        }

        // Releasing a snapshot object may do compaction on vset's versions.
        ~Snapshot() override
        {
            if (auto handle = compact_handle.lock(); handle)
            {
                // increase the index so that upper level know it should try
                // the version compact.
                vset->last_released_snapshot_index.fetch_add(1);
                // Do vset->compactUntil on background pool
                handle->wake();
            }
            // else if the handle is nullptr (handle is not set or task has been removed from bkg pool),
            // just skip the version list compact.

            // Remove snapshot from linked list
            view.release();

            CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots);
        }

        const PageEntriesView * version() const { return &view; }

        // The time this snapshot living for
        double elapsedSeconds() const
        {
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> diff = end - create_time;
            return diff.count();
        }

        friend class PageEntriesVersionSetWithDelta;
    };

    using SnapshotPtr = std::shared_ptr<Snapshot>;
    using SnapshotWeakPtr = std::weak_ptr<Snapshot>;

    SnapshotPtr getSnapshot(const String & tracing_id, BackgroundProcessingPool::TaskHandle handle);

    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(
        PageEntriesEdit & edit,
        bool need_scan_page_ids = true);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> //
    listAllLiveFiles(std::unique_lock<std::shared_mutex> &&, bool need_scan_page_ids = true);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    void appendVersion(VersionPtr && v, const std::unique_lock<std::shared_mutex> & lock);

    enum class RebaseResult
    {
        SUCCESS,
        INVALID_VERSION,
    };

    /// Use after do compact on VersionList, rebase all
    /// successor Version of Version{`old_base`} onto Version{`new_base`}.
    /// Specially, if no successor version of Version{`old_base`}, which
    /// means `current`==`old_base`, replace `current` with `new_base`.
    /// Examples:
    /// ┌────────────────────────────────┬───────────────────────────────────┐
    /// │         Before rebase          │           After rebase            │
    /// ├────────────────────────────────┼───────────────────────────────────┤
    /// │ Va    <-   Vb  <-    Vc        │      Vd     <-   Vc               │
    /// │       (old_base)  (current)    │   (new_base)    (current)         │
    /// ├────────────────────────────────┼───────────────────────────────────┤
    /// │ Va    <- Vb    <-    Vc        │           Vd                      │
    /// │             (current,old_base) │     (current, new_base)           │
    /// └────────────────────────────────┴───────────────────────────────────┘
    /// Caller should ensure old_base is in VersionSet's link
    RebaseResult rebase(const VersionPtr & old_base, const VersionPtr & new_base);

    std::unique_lock<std::shared_mutex> acquireForLock();

    // Return true if `tail` is in current version-list
    bool isValidVersion(VersionPtr tail) const;

    // If `tail` is in the latest versions-list, do compaction on version-list [head, tail].
    // If there some versions after tail, use vset's `rebase` to concat those version to the
    // new compacted version-list.
    void compactUntil(VersionPtr tail);

    // Scan over all `snapshots`, remove the invalid snapshots and get some statistics
    // of all living snapshots and the oldest living snapshot.
    // Return < num of snapshots,
    //          living time(seconds) of the oldest snapshot,
    //          created thread id of the oldest snapshot      >
    SnapshotsStatistics removeExpiredSnapshots() const;

    static void collectLiveFilesFromVersionList( //
        const PageEntriesView & view,
        std::set<PageFileIdAndLevel> & live_files,
        std::set<PageId> & live_normal_pages,
        bool need_scan_page_ids);

private:
    std::atomic<UInt64> last_released_snapshot_index{0};
    std::atomic<UInt64> last_try_compact_index{0};

    mutable std::shared_mutex read_write_mutex;
    VersionPtr current;
    mutable std::list<SnapshotWeakPtr> snapshots;
    const MVCC::VersionSetConfig config;
    const String name;
    LoggerPtr log;
};

/// Read old entries state from `view_` and apply new edit to `view_->tail`
class DeltaVersionEditAcceptor
{
public:
    explicit DeltaVersionEditAcceptor(
        const PageEntriesView * view_, //
        const String & name_,
        bool ignore_invalid_ref_ = false,
        LoggerPtr log_ = nullptr);

    ~DeltaVersionEditAcceptor();

    void apply(PageEntriesEdit & edit);

    static void applyInplace(
        const String & name,
        const PageEntriesVersionSetWithDelta::VersionPtr & current,
        const PageEntriesEdit & edit,
        LoggerPtr log);

    void gcApply(PageEntriesEdit & edit) { PageEntriesBuilder::gcApplyTemplate(view, edit, current_version); }

    static void gcApplyInplace( //
        const PageEntriesVersionSetWithDelta::VersionPtr & current,
        PageEntriesEdit & edit)
    {
        assert(current->isBase());
        assert(current.use_count() == 1);
        PageEntriesBuilder::gcApplyTemplate(current, edit, current);
    }

private:
    // Read old state from `view` and apply new edit to `current_version`

    void applyPut(PageEntriesEdit::EditRecord & record);
    void applyDel(PageEntriesEdit::EditRecord & record);
    void applyRef(PageEntriesEdit::EditRecord & record);
    void decreasePageRef(PageId page_id);

private:
    PageEntriesView * view;
    PageEntriesVersionSetWithDelta::VersionPtr current_version;
    bool ignore_invalid_ref;

    const String & name;
    LoggerPtr log;
};

} // namespace DB::PS::V2

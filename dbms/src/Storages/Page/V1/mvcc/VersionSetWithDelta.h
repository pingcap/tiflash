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

#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/V1/mvcc/VersionSet.h>
#include <stdint.h>

#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <unordered_set>

namespace ProfileEvents
{
extern const Event PSMVCCCompactOnDelta;
extern const Event PSMVCCCompactOnDeltaRebaseRejected;
extern const Event PSMVCCCompactOnBase;
extern const Event PSMVCCApplyOnCurrentBase;
extern const Event PSMVCCApplyOnCurrentDelta;
extern const Event PSMVCCApplyOnNewDelta;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
} // namespace CurrentMetrics

namespace DB::PS::V1
{
namespace MVCC
{
/// Base type for VersionType of VersionSetWithDelta
template <typename T>
struct MultiVersionCountableForDelta
{
public:
    std::shared_ptr<T> prev;

public:
    explicit MultiVersionCountableForDelta()
        : prev(nullptr)
    {}

    virtual ~MultiVersionCountableForDelta() = default;
};

/// \tparam TVersion         -- Single version on version-list. Require for a `prev` member, see `MultiVersionDeltaCountable`
/// \tparam TVersionView     -- A view to see a list of versions as a single version
/// \tparam TVersionEdit     -- Changes to apply to version set for generating new version
/// \tparam TEditAcceptor    -- Accept a read view and apply edits to new version
template < //
    typename TVersion,
    typename TVersionView,
    typename TVersionEdit,
    typename TEditAcceptor>
class VersionSetWithDelta
{
public:
    using EditAcceptor = TEditAcceptor;
    using VersionType = TVersion;
    using VersionPtr = std::shared_ptr<VersionType>;

public:
    explicit VersionSetWithDelta(const MVCC::VersionSetConfig & config_, Poco::Logger * log_)
        : current(std::move(VersionType::createBase()))
        , //
        snapshots(std::move(std::make_shared<Snapshot>(this, nullptr)))
        , //
        config(config_)
        , log(log_)
    {
    }

    virtual ~VersionSetWithDelta()
    {
        current.reset();
        // snapshot list is empty
        assert(snapshots->prev == snapshots.get());
    }

    void apply(TVersionEdit & edit)
    {
        std::unique_lock read_lock(read_write_mutex);

        if (current.use_count() == 1 && current->isBase())
        {
            ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnCurrentBase);
            // If no readers, we could directly merge edits.
            TEditAcceptor::applyInplace(current, edit, log);
            return;
        }

        if (current.use_count() != 1)
        {
            ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnNewDelta);
            // There are reader(s) on current, generate new delta version and append to version-list
            VersionPtr v = VersionType::createDelta();
            appendVersion(std::move(v), read_lock);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnCurrentDelta);
        }
        // Make a view from head to new version, then apply edits on `current`.
        auto view = std::make_shared<TVersionView>(current);
        EditAcceptor builder(view.get(), /* ignore_invalid_ref_= */ true, log);
        builder.apply(edit);
    }

public:
    /// Snapshot.
    /// When snapshot object is free, it will call `view.release()` to compact VersionList,
    /// and remove itself from VersionSet's snapshots list.
    class Snapshot : private boost::noncopyable
    {
    public:
        VersionSetWithDelta * vset;
        TVersionView view;

        Snapshot * prev;
        Snapshot * next;

    public:
        Snapshot(VersionSetWithDelta * vset_, VersionPtr tail_)
            : vset(vset_)
            , view(std::move(tail_))
            , prev(this)
            , next(this)
        {}

        ~Snapshot()
        {
            vset->compactOnDeltaRelease(view.transferTailVersionOwn());
            // Remove snapshot from linked list
            std::unique_lock lock = vset->acquireForLock();
            prev->next = next;
            next->prev = prev;
        }

        const TVersionView * version() const { return &view; }

        template <typename V, typename VV, typename VE, typename B>
        friend class VersionSetWithDelta;
    };

    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version.
    /// call `snapshot.reset()` or let `snapshot` gone if you don't need it anymore.
    SnapshotPtr getSnapshot()
    {
        // acquire for unique_lock since we need to add all snapshots to link list
        std::unique_lock<std::shared_mutex> lock(read_write_mutex);

        auto s = std::make_shared<Snapshot>(this, current);
        // Register snapshot to VersionSet
        s->prev = snapshots->prev;
        s->next = snapshots.get();
        snapshots->prev->next = s.get();
        snapshots->prev = s.get();
        return s;
    }

protected:
    void appendVersion(VersionPtr && v, const std::unique_lock<std::shared_mutex> & lock)
    {
        (void)lock; // just for ensure lock is hold
        assert(v != current);
        // Append to linked list
        v->prev = current;
        current = v;
    }

protected:
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
    RebaseResult rebase(const VersionPtr & old_base, const VersionPtr & new_base)
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

    std::unique_lock<std::shared_mutex> acquireForLock() { return std::unique_lock<std::shared_mutex>(read_write_mutex); }

    // Return true if `tail` is in current version-list
    bool isValidVersion(const VersionPtr tail) const
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

    // If `tail` is in current
    // Do compaction on version-list [head, tail]. If there some versions after tail, use vset's `rebase` to concat them.
    void compactOnDeltaRelease(VersionPtr && tail)
    {
        do
        {
            if (tail == nullptr || tail->isBase())
            {
                break;
            }
            {
                // If we can not found tail from `current` version-list, then other view has already
                // do compaction on `tail` version, and we can just free that version
                std::shared_lock lock(read_write_mutex);
                if (!isValidVersion(tail))
                    break;
            }
            // do compact on delta
            ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDelta);
            VersionPtr tmp = VersionType::compactDeltas(tail); // Note: May be compacted by different threads
            if (tmp != nullptr)
            {
                // rebase vset->current on `this->tail` to base on `tmp`
                if (this->rebase(tail, tmp) == RebaseResult::INVALID_VERSION)
                {
                    // Another thread may have done compaction and rebase, then we just release `tail`
                    ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDeltaRebaseRejected);
                    break;
                }
                // release tail ref on this view, replace with tmp
                tail = tmp;
                tmp.reset();
            }
            // do compact on base
            if (tail->shouldCompactToBase(config))
            {
                ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnBase);
                auto old_base = std::atomic_load(&tail->prev);
                assert(old_base != nullptr);
                VersionPtr new_base = VersionType::compactDeltaAndBase(old_base, tail);
                // replace nodes [head, tail] -> new_base
                if (this->rebase(tail, new_base) == RebaseResult::INVALID_VERSION)
                {
                    // Another thread may have done compaction and rebase, then we just release `tail`. In case we may add more code after do compaction on base
                    ProfileEvents::increment(ProfileEvents::PSMVCCCompactOnDeltaRebaseRejected);
                    break;
                }
            }
        } while (false);
        tail.reset();
    }

public:
    /// Some helper functions

    size_t size() const
    {
        std::shared_lock read_lock(read_write_mutex);
        return sizeUnlocked();
    }

    size_t sizeUnlocked() const
    {
        size_t sz = 0;
        for (auto v = current; v != nullptr; v = std::atomic_load(&v->prev))
        {
            sz += 1;
        }
        return sz;
    }

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

protected:
    mutable std::shared_mutex read_write_mutex;
    VersionPtr current;
    SnapshotPtr snapshots;
    MVCC::VersionSetConfig config;
    Poco::Logger * log;
};

} // namespace MVCC
} // namespace DB::PS::V1

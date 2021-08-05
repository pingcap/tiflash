#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteHelpers.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Storages/Page/mvcc/VersionSet.h>
#include <stdint.h>

#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <unordered_set>

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
extern const Event PSMVCCApplyOnCurrentBase;
extern const Event PSMVCCApplyOnCurrentDelta;
extern const Event PSMVCCApplyOnNewDelta;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric PSMVCCNumSnapshots;
extern const Metric PSMVCCSnapshotsList;
} // namespace CurrentMetrics

namespace DB
{

namespace FailPoints
{
extern const char random_slow_page_storage_remove_expired_snapshots[];
} // namespace FailPoints

namespace MVCC
{
/// Base type for VersionType of VersionSetWithDelta
template <typename T>
struct MultiVersionCountableForDelta
{
public:
    std::shared_ptr<T> prev;

public:
    explicit MultiVersionCountableForDelta() : prev(nullptr) {}

    virtual ~MultiVersionCountableForDelta() = default;
};

// TODO: Merge `VersionSetWithDelta` with `PageEntriesVersionSetWithDelta`, template make things
//       more complicated and hard to understand.
//
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
    using VersionType  = TVersion;
    using VersionPtr   = std::shared_ptr<VersionType>;

public:
    explicit VersionSetWithDelta(String name_, const ::DB::MVCC::VersionSetConfig & config_, Poco::Logger * log_)
        : current(std::move(VersionType::createBase())), //
          snapshots(),                                   //
          config(config_),
          name(std::move(name_)),
          log(log_)
    {
    }

    virtual ~VersionSetWithDelta()
    {
        current.reset();

        removeExpiredSnapshots();

        // snapshot list is empty
        assert(snapshots.empty());
    }

    void apply(TVersionEdit & edit)
    {
        std::unique_lock read_lock(read_write_mutex);

        if (current.use_count() == 1 && current->isBase())
        {
            ProfileEvents::increment(ProfileEvents::PSMVCCApplyOnCurrentBase);
            // If no readers, we could directly merge edits.
            TEditAcceptor::applyInplace(name, current, edit, log);
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
        auto         view = std::make_shared<TVersionView>(current);
        EditAcceptor builder(view.get(), name, /* ignore_invalid_ref_= */ true, log);
        builder.apply(edit);
    }

public:
    /// Snapshot.
    /// When snapshot object is free, it will call `view.release()` to compact VersionList,
    /// and its weak_ptr will be from VersionSet's snapshots list.
    class Snapshot : private boost::noncopyable
    {
    public:
        VersionSetWithDelta * vset;
        TVersionView          view;

        using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
        const unsigned t_id;

    private:
        const TimePoint create_time;

    public:
        Snapshot(VersionSetWithDelta * vset_, VersionPtr tail_)
            : vset(vset_), view(std::move(tail_)), t_id(Poco::ThreadNumber::get()), create_time(std::chrono::steady_clock::now())
        {
            CurrentMetrics::add(CurrentMetrics::PSMVCCNumSnapshots);
        }

        // Releasing a snapshot object may do compaction on vset's versions.
        ~Snapshot()
        {
            vset->compactOnDeltaRelease(view.getSharedTailVersion());
            // Remove snapshot from linked list

            view.release();

            CurrentMetrics::sub(CurrentMetrics::PSMVCCNumSnapshots);
        }

        const TVersionView * version() const { return &view; }

        // The time this snapshot living for
        double elapsedSeconds() const
        {
            auto                          end  = std::chrono::steady_clock::now();
            std::chrono::duration<double> diff = end - create_time;
            return diff.count();
        }

        template <typename V, typename VV, typename VE, typename B>
        friend class VersionSetWithDelta;
    };

    using SnapshotPtr     = std::shared_ptr<Snapshot>;
    using SnapshotWeakPtr = std::weak_ptr<Snapshot>;

    /// Create a snapshot for current version.
    /// call `snapshot.reset()` or let `snapshot` gone if you don't need it anymore.
    SnapshotPtr getSnapshot()
    {
        // acquire for unique_lock since we need to add all snapshots to link list
        std::unique_lock<std::shared_mutex> lock(read_write_mutex);

        auto s = std::make_shared<Snapshot>(this, current);
        // Register a weak_ptr to snapshot into VersionSet so that we can get all living PageFiles
        // by `PageEntriesVersionSetWithDelta::listAllLiveFiles`, and it remove useless weak_ptr of snapshots.
        // Do not call `vset->removeExpiredSnapshots` inside `~Snapshot`, or it may cause incursive deadlock
        // on `vset->read_write_mutex`.
        snapshots.emplace_back(SnapshotWeakPtr(s));
        CurrentMetrics::add(CurrentMetrics::PSMVCCSnapshotsList);
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

    // If `tail` is in the latest versions-list, do compaction on version-list [head, tail].
    // If there some versions after tail, use vset's `rebase` to concat them.
    void compactOnDeltaRelease(VersionPtr tail)
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
        VersionPtr tmp = VersionType::compactDeltas(tail); // Note: May be compacted by different threads
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
                return;
            }
        }
    }

private:
    // Scan over all `snapshots`, remove the invalid snapshots and get some statistics
    // of all living snapshots and the oldest living snapshot.
    // Return < num of snapshots,
    //          living time(seconds) of the oldest snapshot,
    //          created thread id of the oldest snapshot      >
    std::tuple<size_t, double, unsigned> removeExpiredSnapshots() const
    {
        // Notice: we should free those valid snapshots without locking, or it may cause
        // incursive deadlock on `vset->read_write_mutex`.
        std::vector<SnapshotPtr> valid_snapshots;
        double                   longest_living_seconds        = 0.0;
        unsigned                 longest_living_from_thread_id = 0;
        DB::Int64                num_snapshots_removed         = 0;
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
                        pcg64                     rng(randomSeed());
                        std::chrono::milliseconds ms{std::uniform_int_distribution(0, 900)(rng)}; // 0~900 milliseconds
                        std::this_thread::sleep_for(ms);
                    });
                    const auto snapshot_lifetime = snapshot_or_invalid->elapsedSeconds();
                    if (snapshot_lifetime > longest_living_seconds)
                    {
                        longest_living_seconds        = snapshot_lifetime;
                        longest_living_from_thread_id = snapshot_or_invalid->t_id;
                    }
                    valid_snapshots.emplace_back(snapshot_or_invalid); // Save valid snapshot and release them without lock later
                    iter++;
                }
            }
        } // unlock `read_write_mutex`

        const size_t num_valid_snapshots = valid_snapshots.size();
        valid_snapshots.clear();

        CurrentMetrics::sub(CurrentMetrics::PSMVCCSnapshotsList, num_snapshots_removed);
        // Return some statistics of the oldest living snapshot.
        return {num_valid_snapshots, longest_living_seconds, longest_living_from_thread_id};
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

    std::tuple<size_t, double, unsigned> getSnapshotsStat() const
    {
        // Note: this will scan and remove expired weak_ptrs from `snapshots`
        return removeExpiredSnapshots();
    }

    std::string toDebugString() const
    {
        std::shared_lock lock(read_write_mutex);
        return versionToDebugString(current);
    }

    static std::string versionToDebugString(VersionPtr tail)
    {
        std::string            s;
        bool                   is_first = true;
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
    mutable std::shared_mutex          read_write_mutex;
    VersionPtr                         current;
    mutable std::list<SnapshotWeakPtr> snapshots;
    const ::DB::MVCC::VersionSetConfig config;
    const String                       name;
    Poco::Logger *                     log;
};

} // namespace MVCC
} // namespace DB

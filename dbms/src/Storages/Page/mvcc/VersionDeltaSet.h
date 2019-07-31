#pragma once

#include <stdint.h>
#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <unordered_set>

#include <IO/WriteHelpers.h>
#include <Storages/Page/mvcc/VersionSet.h>

namespace DB
{
namespace MVCC
{
/// Base type for VersionType of VersionDeltaSet
template <typename T>
struct MultiVersionDeltaCountable
{
public:
    std::shared_ptr<T> prev;

public:
    explicit MultiVersionDeltaCountable() : prev(nullptr) {}

    virtual ~MultiVersionDeltaCountable() = default;
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
class VersionDeltaSet
{
public:
    using EditAcceptor = TEditAcceptor;
    using VersionType  = TVersion;
    using VersionPtr   = std::shared_ptr<VersionType>;

public:
    explicit VersionDeltaSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig())
        : current(std::move(VersionType::createBase())),                   //
          snapshots(std::move(std::make_shared<Snapshot>(this, nullptr))), //
          config(config_)
    {
    }

    virtual ~VersionDeltaSet()
    {
        assert(snapshots->prev == snapshots.get()); // snapshot list is empty
        current.reset();
    }

    void apply(TVersionEdit & edit)
    {
        std::unique_lock read_lock(read_mutex);

        if (current.use_count() == 1 && current->isBase())
        {
            // If no readers, we could directly merge edits.
            TEditAcceptor::applyInplace(current, edit);
        }
        else
        {
            if (current.use_count() != 1)
            {
                // There are reader(s) on current, generate new delta version and append to version-list
                VersionPtr v = VersionType::createDelta();
                appendVersion(std::move(v));
            }
            // Make a view from head to new version, then apply edits on `current`.
            auto         view = std::make_shared<TVersionView>(current);
            EditAcceptor builder(view.get());
            builder.apply(edit);
        }
    }

public:
    /// Snapshot.
    /// When snapshot object is free, it will call `view.release()` to compact VersionList,
    /// and remove itself from VersionSet's snapshots list.
    class Snapshot
    {
    public:
        VersionDeltaSet * vset;
        TVersionView      view;

        Snapshot * prev;
        Snapshot * next;

    public:
        Snapshot(VersionDeltaSet * vset_, VersionPtr tail_) : vset(vset_), view(std::move(tail_)), prev(this), next(this) {}

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
        friend class VersionDeltaSet;
    };

    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version.
    /// call `snapshot.reset()` or let `snapshot` gone if you don't need it anymore.
    SnapshotPtr getSnapshot()
    {
        // acquire for unique_lock since we need to add all snapshots to link list
        std::unique_lock<std::shared_mutex> lock(read_mutex);
        auto                                s = std::make_shared<Snapshot>(this, current);
        // Register snapshot to VersionSet
        s->prev               = snapshots->prev;
        s->next               = snapshots.get();
        snapshots->prev->next = s.get();
        snapshots->prev       = s.get();
        return s;
    }

protected:
    void appendVersion(VersionPtr && v)
    {
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
        std::unique_lock lock(read_mutex);
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

        auto q = current, p = current->prev;
        while (p != nullptr && p != old_base)
        {
            q = p;
            p = q->prev;
        }
        // p must point to `old_base` now
        assert(p == old_base);
        // rebase q on `new_base`
        q->prev = new_base;
        return RebaseResult::SUCCESS;
    }

    std::unique_lock<std::shared_mutex> acquireForLock() { return std::unique_lock<std::shared_mutex>(read_mutex); }

    // Return true if `tail` is in current version-list
    bool isValidVersion(const VersionPtr tail) const
    {
        for (auto node = current; node != nullptr; node = node->prev)
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
            // If we can not found tail from `current` version-list, then other view has already
            // do compaction on `tail` version, and we can just free that version
            if (!isValidVersion(tail))
            {
                break;
            }
            // do compact on delta
            VersionPtr tmp = compactDeltas(tail); // Note: May be compacted by different threads
            if (tmp != nullptr)
            {
                // rebase vset->current on `this->tail` to base on `tmp`
                if (this->rebase(tail, tmp) == RebaseResult::INVALID_VERSION)
                {
                    // Another thread may have done compaction and rebase, then we just release `tail`
                    break;
                }
                // release tail ref on this view, replace with tmp
                tail = tmp;
                tmp.reset();
            }
            // do compact on base
            if (tail->shouldCompactToBase(config))
            {
                auto old_base = tail->prev;
                assert(old_base != nullptr);
                VersionPtr new_base = compactDeltaAndBase(old_base, tail);
                // replace nodes [head, tail] -> new_base
                if (this->rebase(tail, new_base) == RebaseResult::INVALID_VERSION)
                {
                    // Another thread may have done compaction and rebase, then we just release `tail`. In case we may add more code after do compaction on base
                    break;
                }
            }
        } while (false);
        tail.reset();
    }

    virtual VersionPtr compactDeltas(const VersionPtr & tail) const = 0;

    virtual VersionPtr compactDeltaAndBase(const VersionPtr & old_base, VersionPtr & delta) const = 0;

public:
    /// Some helper functions

    size_t size() const
    {
        std::unique_lock read_lock(read_mutex);
        return sizeUnlocked();
    }

    size_t sizeUnlocked() const
    {
        size_t sz = 0;
        for (auto v = current; v != nullptr; v = v->prev)
        {
            sz += 1;
        }
        return sz;
    }

    std::string toDebugStringUnlocked() const { return versionToDebugString(current); }

    static std::string versionToDebugString(VersionPtr tail)
    {
        std::string            s;
        bool                   is_first = true;
        std::stack<VersionPtr> deltas;
        for (auto v = tail; v != nullptr; v = v->prev)
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
            s += ",\"addr\":", s += DB::pToString(v.get());
            s += '}';
        }
        return s;
    }

protected:
    mutable std::shared_mutex    read_mutex;
    VersionPtr                   current;
    SnapshotPtr                  snapshots;
    ::DB::MVCC::VersionSetConfig config;
};

} // namespace MVCC
} // namespace DB

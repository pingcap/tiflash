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

/// Base component for Snapshot of VersionDeltaSet.
/// When view `release()` called, it will do compact on version-list
///
/// \tparam VersionSet_t
///   members required:
///       config
///   functions required:
///       void rebase(const VersionPtr & old_base, const VersionPtr & new_base)
/// \tparam Builder_t
///   functions required:
///       VersionPtr compactDeltas(const VersionPtr &tail);
///       bool needCompactToBase(const VersionSetConfig *config, const VersionPtr &delta)
///       VersionPtr compactDeltaAndBase(const VersionPtr &old_base, const VersionPtr &delta)
template <typename VersionSet_t, typename Builder_t>
struct VersionViewBase
{
public:
    VersionSet_t *                    vset;
    typename VersionSet_t::VersionPtr tail;

public:
    VersionViewBase(VersionSet_t * vset_, typename VersionSet_t::VersionPtr tail_) : vset(vset_), tail(std::move(tail_)) {}

    // Just let the reference of `tail` go
    virtual ~VersionViewBase() {}

    // Do compaction on version-list [head, tail]. If there some versions after tail,
    // use vset's `rebase` to concat them.
    void release()
    {
        if (tail == nullptr || tail->isBase())
            return;
        // do compact on delta
        typename VersionSet_t::VersionPtr tmp = Builder_t::compactDeltas(tail);
        if (tmp != nullptr)
        {
            // rebase vset->current on `this->tail` to base on `tmp`
            vset->rebase(tail, tmp);
            // release tail ref on this view, replace with tmp
            tail = tmp;
            tmp.reset();
        }
        // do compact on base
        bool is_compact_delta_to_base = Builder_t::needCompactToBase(vset->config, tail);
        if (is_compact_delta_to_base)
        {
            auto old_base = tail->prev;
            assert(old_base != nullptr);
            typename VersionSet_t::VersionPtr new_base = Builder_t::compactDeltaAndBase(old_base, tail);
            // replace nodes [head, tail] -> new_base
            vset->rebase(tail, new_base);
        }
    }
};


template < //
    typename Version_t,
    typename VersionView_t,
    typename VersionEdit_t,
    typename Builder_t>
class VersionDeltaSet
{
public:
    using BuilderType = Builder_t;
    using VersionType = Version_t;
    using VersionPtr  = std::shared_ptr<VersionType>;

public:
    explicit VersionDeltaSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig())
        : current(std::move(VersionType::createBase())),                                //
          snapshots(std::move(std::make_shared<Snapshot>(this, nullptr, &read_mutex))), //
          config(config_)
    {
    }

    virtual ~VersionDeltaSet()
    {
        assert(snapshots->prev == snapshots.get()); // snapshot list is empty
        current.reset();
    }

    void restore(VersionPtr && v)
    {
        std::unique_lock read_lock(read_mutex);
        // check should only call when there is nothing
        assert(current->empty());
        assert(current->isBase());
        assert(current->prev == nullptr);
        v->is_base = true;
        current.swap(v);
        v.reset();
        assert(current->isBase());
    }

    void apply(VersionEdit_t & edit)
    {
        std::unique_lock read_lock(read_mutex);

        if (current.use_count() == 1 && current->isBase())
        {
            // If no readers, we could directly merge edits.
            BuilderType::applyInplace(current, edit);
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
            auto      view = std::make_shared<VersionView_t>(this, current);
            Builder_t builder(view.get());
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
        VersionView_t       view;
        std::shared_mutex * mutex;

        Snapshot * prev;
        Snapshot * next;

    public:
        Snapshot(VersionDeltaSet *   vset_,
                 VersionPtr          tail_, //
                 std::shared_mutex * mutex_)
            : view(vset_, std::move(tail_)), mutex(mutex_), prev(this), next(this)
        {
        }

        ~Snapshot()
        {
            std::unique_lock lock(*mutex);
            view.release();
            // Remove from linked list
            prev->next = next;
            next->prev = prev;
        }

        const VersionView_t * version() const { return &view; }

        template <typename V_t, typename VV_t, typename VE_t, typename B_t>
        friend class VersionDeltaSet;
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version.
    /// call `snapshot.reset()` or let `snapshot` gone if you don't need it anymore.
    SnapshotPtr getSnapshot()
    {
        // acquire for unique_lock since we need to add all snapshots to link list
        std::unique_lock<std::shared_mutex> lock(read_mutex);
        auto                                s = std::make_shared<Snapshot>(this, current, &read_mutex);
        // Register snapshot to VersionSet
        s->prev               = snapshots->prev;
        s->next               = snapshots.get();
        snapshots->prev->next = s.get();
        snapshots->prev       = s.get();
        return s;
    }

public:
    mutable std::shared_mutex    read_mutex;
    VersionPtr                   current;
    SnapshotPtr                  snapshots;
    ::DB::MVCC::VersionSetConfig config;

protected:
    template <typename VS_t, typename B_t>
    friend struct VersionViewBase;

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
    void rebase(const VersionPtr & old_base, const VersionPtr & new_base)
    {
        assert(old_base != nullptr);
        if (old_base == current)
        {
            current = new_base;
            return;
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
    }

    void appendVersion(VersionPtr && v)
    {
        assert(v != current);
        // Append to linked list
        v->prev = current;
        current = v;
    }

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
            sz += 1;
        return sz;
    }

    std::string toDebugStringUnlocked() const
    {
        std::string            s;
        bool                   is_first = true;
        std::stack<VersionPtr> deltas;
        for (auto v = current; v != nullptr; v = v->prev)
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
};

} // namespace MVCC
} // namespace DB

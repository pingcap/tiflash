#pragma once

#include <stdint.h>
#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <unordered_set>

#include <Common/VersionSet.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace MVCC
{

template <typename T>
struct MultiVersionDeltaCountable
{
public:
    std::shared_ptr<T> prev;

public:
    explicit MultiVersionDeltaCountable() : prev(nullptr) {}
    virtual ~MultiVersionDeltaCountable() = default;
};

template <typename VersionSet_t, typename VersionDelta_t, typename Builder_t>
struct VersionViewBase
{
public:
    VersionSet_t * vset;
    typename VersionSet_t::VersionDeltaPtr tail;

public:
    VersionViewBase(VersionSet_t * vset_, std::shared_ptr<VersionDelta_t> tail_) : vset(vset_), tail(std::move(tail_)) {}

    void release()
    {
        if (tail == nullptr || tail->isBase())
            return;
        // do compact on delta
        std::shared_ptr<VersionDelta_t> tmp = Builder_t::compactDeltas(vset, tail);
        if (tmp != nullptr)
        {
            // rebase vset->current on `this->tail` to base on `tmp`
            vset->rebase(tail, tmp);
            // release tail ref on this view, replace with tmp
            tail = tmp;
            tmp.reset();
        }
        // do compact on base
        bool is_compact_delta_to_base = Builder_t::needCompactToBase(vset, tail);
        if (is_compact_delta_to_base)
        {
            auto old_base = tail->prev;
            if (old_base != nullptr)
            {
                typename VersionSet_t::VersionBasePtr new_base = Builder_t::compactDeltaAndBase(old_base, tail);
                // replace nodes [head, tail] -> new_base
                vset->rebase(tail, new_base);
            }
        }
    }
};


template <                                           //
    typename VersionBase_t, typename VersionDelta_t, //
    typename VersionView_t,                          //
    typename VersionEdit_t, typename Builder_t>
class VersionDeltaSet
{
public:
    using BuilderType = Builder_t;
    using VersionBaseType = VersionBase_t;
    using VersionBasePtr = std::shared_ptr<VersionBaseType>;
    using VersionDeltaType = VersionDelta_t;
    using VersionDeltaPtr = std::shared_ptr<VersionDeltaType>;

public:
    explicit VersionDeltaSet(const ::DB::MVCC::VersionSetConfig &config_ = ::DB::MVCC::VersionSetConfig())
        : current(std::move(VersionBaseType::createBase())),                            //
          snapshots(std::move(std::make_shared<Snapshot>(this, nullptr, &read_mutex))), //
          config(config_)
    {}

    virtual ~VersionDeltaSet()
    {
        assert(snapshots->prev == snapshots.get());  // snapshot list is empty
        current.reset();
    }

    void restore(std::shared_ptr<VersionDelta_t> && v)
    {
        std::unique_lock read_lock(read_mutex);
        assert(current->empty());
        Builder_t::mergeDeltaToBaseInplace(current, std::move(v));
    }

    void apply(VersionEdit_t & edit)
    {
        std::unique_lock read_lock(read_mutex);

        // TODO if no readers, we could not generate a view?
        // apply edit base on base_view
        std::shared_ptr<VersionDelta_t> v;
        {
            auto base_view = std::make_shared<VersionView_t>(this, current);
            Builder_t builder(base_view.get());
            builder.apply(edit);
            v = builder.build();
        }

        if (current.use_count() == 1)
        {
            if (current->isBase() && current.use_count() == 1)
            {
                // merge new delta to base version
                std::cerr << "merge to base" << std::endl;
                Builder_t::mergeDeltaToBaseInplace(current, std::move(v));
            }
            else
            {
                // merge new delta to current version
                std::cerr << "merge to prev delta" << std::endl;
                current->merge(*v);
            }
        }
        else
        {
            appendVersion(std::move(v));
        }
    }

public:
    /// Snapshot
    class Snapshot
    {
    public:
        VersionView_t view;
        std::shared_mutex * mutex;

        Snapshot * prev;
        Snapshot * next;

    public:
        Snapshot(VersionDeltaSet * vset_, std::shared_ptr<VersionDelta_t> tail_, //
            std::shared_mutex * mutex_)
            : view(vset_, std::move(tail_)), mutex(mutex_), prev(this), next(this)
        {}

        ~Snapshot()
        {
            std::unique_lock lock(*mutex);
            view.release();
            // Remove from linked list
            prev->next = next;
            next->prev = prev;
        }

        const VersionView_t * version() const { return &view; }

        template <typename VB_t, typename VD_t, typename VV_t, typename VE_t, typename B_t>
        friend class VersionDeltaSet;
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version
    SnapshotPtr getSnapshot()
    {
        // acquire for unique_lock since we need to add all snapshots to link list
        std::unique_lock<std::shared_mutex> lock(read_mutex);
        auto s = std::make_shared<Snapshot>(this, current, &read_mutex);
        // Register snapshot to VersionSet
        s->prev = snapshots->prev;
        s->next = snapshots.get();
        snapshots->prev->next = s.get();
        snapshots->prev = s.get();
        return s;
    }

public:
    mutable std::shared_mutex read_mutex;
    std::shared_ptr<VersionDelta_t> current;
    SnapshotPtr snapshots;
    ::DB::MVCC::VersionSetConfig config;

protected:
    template <typename VS_t, typename VD_t, typename B_t>
    friend struct VersionViewBase;

    /// Rebase all successor Version of Version{`old_base`} onto Version{`new_base`}.
    /// Specially, if no successor version of Version{`old_base`}, which
    /// means `current`==`old_base`, replace `current` with `new_base`.
    /// Examples:
    /// ┌────────────────────────────────┬───────────────────────────────────┐
    /// │ Va    <-   Vb  <-    Vc        │      Vd     <-   Vc               │
    /// │       (old_base)  (current)    │   (new_base)    (current)         │
    /// ├────────────────────────────────┼───────────────────────────────────┤
    /// │ Va    <- Vb    <-    Vc        │           Vd                      │
    /// │             (current,old_base) │     (current, new_base)           │
    /// └────────────────────────────────┴───────────────────────────────────┘
    /// caller should ensure old_base is in VersionSet's link
    void rebase(const std::shared_ptr<VersionDelta_t> & old_base, const std::shared_ptr<VersionDelta_t> & new_base)
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

    void appendVersion(std::shared_ptr<VersionDelta_t> &&v)
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
        std::string s;
        bool is_first = true;
        std::stack<std::shared_ptr<VersionDelta_t>> deltas;
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

#pragma once

#include <stdint.h>
#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <stack>

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
    explicit MultiVersionDeltaCountable(T * self) : prev(nullptr) { (void)self; }
    virtual ~MultiVersionDeltaCountable() {}
};

template <typename VersionSet_t, typename VersionDelta_t, typename Builder_t>
struct VersionViewBase
{
public:
    VersionSet_t * vset;
    std::shared_ptr<VersionDelta_t> tail;

public:
    VersionViewBase(VersionSet_t * vset_, std::shared_ptr<VersionDelta_t> tail_) : vset(vset_), tail(std::move(tail_)) {}

    void incrRefCount() {}

    void decrRefCount(std::shared_mutex & mutex)
    {
        std::unique_lock lock(mutex);
        decrRefCount();
    }

    void decrRefCount()
    {
        // do compact on delta
        auto tmp = Builder_t::mergeDeltas(vset, tail);
        if (tmp != nullptr)
        {
            // replace nodes (head, tail] -> tmp
            tmp->prev = nullptr;
            vset->current = tmp;

            // release tail ref on this view, replace with tmp
            tail.reset(tmp);
        }
        // TODO do compact on base
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

public:
    VersionDeltaSet()
        : base(std::move(std::make_shared<VersionBase_t>())), current(nullptr), snapshots(std::move(std::make_shared<Snapshot>()))
    {
        // add ref count of VersionBase
        // append a init version to link
        appendVersion(std::move(std::make_shared<VersionDelta_t>()));
    }

    virtual ~VersionDeltaSet() = default;

    void restore(std::shared_ptr<VersionDelta_t> && v)
    {
        std::unique_lock read_lock(read_mutex);
        assert(base->empty());
        Builder_t::mergeDeltaToBase(base, std::move(v));
    }

    void apply(VersionEdit_t & edit)
    {
        std::unique_lock read_lock(read_mutex);

        // TODO if no readers, we should not generate a view
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
            if (current->empty() && base.use_count() == 1)
            {
                // merge new delta to base version
                std::cerr << "merge to base" << std::endl;
                Builder_t::mergeDeltaToBase(base, std::move(v));
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
        s += "B:{\"rc\":";
        s += DB::toString(base.use_count());
        s += "},";
        s += "D:";
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
            s += is_first ? "" : "->";
            is_first = false;
            s += "{\"rc\":";
            s += DB::toString(v.use_count() - 1);
            s += '}';
        }
        return s;
    }

public:
    class Snapshot
    {
    private:
        VersionView_t view;
        std::shared_mutex * mutex;

        Snapshot * prev;
        Snapshot * next;

    private:
        Snapshot() : view(), mutex(nullptr), prev(this), next(this) {}
        Snapshot(VersionDeltaSet * vset_, std::shared_ptr<VersionDelta_t> tail_, //
            std::shared_mutex * mutex_)
            : view(vset_, std::move(tail_)), mutex(mutex_)
        {
            view.incrRefCount();
        }

    public:
        ~Snapshot()
        {
            view.decrRefCount(*mutex);
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
        std::shared_lock<std::shared_mutex> lock(read_mutex);
        auto s = std::make_shared<Snapshot>(this, current, &read_mutex);
        // Register snapshot to VersionSet
        s->prev = snapshots->prev;
        s->next = snapshots.get();
        snapshots->prev->next = s.get();
        snapshots->prev = s.get();
        return s;
    }

public:
    std::shared_ptr<VersionBase_t> base;
    std::shared_ptr<VersionDelta_t> current;
    SnapshotPtr snapshots;

    mutable std::shared_mutex read_mutex;

protected:
    void appendVersion(std::shared_ptr<VersionDelta_t> && v)
    {
        assert(v != current);
        // Append to linked list
        v->prev = current;
        current = v;
    }
};

} // namespace MVCC
} // namespace DB

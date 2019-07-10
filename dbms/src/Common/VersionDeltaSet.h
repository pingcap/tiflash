#pragma once

#include <stdint.h>
#include <cassert>
#include <mutex>
#include <shared_mutex>

#include <Common/VersionSet.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace MVCC
{

template <typename VersionSet_t, typename VersionDelta_t, typename Builder_t>
struct VersionViewBase
{
public:
    VersionSet_t * vset;
    VersionDelta_t * tail;

public:
    VersionViewBase(VersionSet_t * vset_, VersionDelta_t * tail_) : vset(vset_), tail(tail_) {}

    void incrRefCount()
    {
        // incr ref count of base, and delta(head, tail]
        vset->base->incrRefCount();
        for (VersionDelta_t * v = tail; v != &vset->placeholder_node; v = v->prev)
            v->incrRefCount();
    }

    void decrRefCount(std::shared_mutex & mutex)
    {
        std::unique_lock lock(mutex);
        decrRefCount();
    }

    void decrRefCount()
    {
        // decr ref count of base, and delta(head, tail]
        vset->base->decrRefCount();
        for (VersionDelta_t * v = tail; v != &vset->placeholder_node; v = v->prev)
            v->ref_count--;
        // TODO do compact on delta then base
        Builder_t::mergeDeltas(vset);
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
    VersionDeltaSet() : base(new VersionBase_t()), placeholder_node(), current(nullptr)
    {
        // add ref count of VersionBase
        base->ref_count = 1;
        // append a init version to link
        appendVersion(new VersionDelta_t);
    }

    virtual ~VersionDeltaSet()
    {
        base->decrRefCount();
        current->decrRefCount();
        assert(placeholder_node.next == &placeholder_node); // List must be empty
    }

    void restore(VersionDelta_t * const v)
    {
        std::unique_lock read_lock(read_mutex);
        assert(base->empty());
        Builder_t::mergeDeltaToBase(base, v);
        delete v;
    }

    void apply(const VersionEdit_t & edit)
    {
        std::unique_lock read_lock(read_mutex);

        auto base_view = std::make_shared<VersionView_t>(this, current);
        // apply edit to delta_base
        VersionDelta_t * v = nullptr;
        {
            Builder_t builder(base_view.get());
            builder.apply(edit);
            v = builder.build();
        }
        base_view.reset();

        if (current->ref_count == 1)
        {
            // merge new delta to current version
            current->merge(*v);
            delete v;
        }
        else
        {
            appendVersion(v);
        }
    }

    size_t size() const
    {
        std::unique_lock read_lock(read_mutex);
        size_t sz = 0;
        for (VersionDelta_t * v = placeholder_node.next; v != &placeholder_node; v = v->next)
            sz += 1;
        return sz;
    }

    size_t sizeUnlocked() const
    {
        size_t sz = 0;
        for (VersionDelta_t * v = placeholder_node.next; v != &placeholder_node; v = v->next)
            sz += 1;
        return sz;
    }

    std::string toDebugStringUnlocked() const
    {
        std::string s;
        s += "B:{\"rc\":";
        s += DB::toString(base->ref_count.load());
        s += "},";
        s += "D:";
        for (VersionDelta_t * v = placeholder_node.next; v != &placeholder_node; v = v->next)
        {
            if (!s.empty())
                s += "->";
            s += "{\"rc\":";
            s += DB::toString(v->ref_count.load());
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

    public:
        Snapshot(VersionDeltaSet * vset_, VersionDelta_t * tail_, //
            std::shared_mutex * mutex_)
            : view(vset_, tail_), mutex(mutex_)
        {
            view.incrRefCount();
        }
        ~Snapshot() { view.decrRefCount(*mutex); }

        const VersionView_t * version() const { return &view; }
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version
    SnapshotPtr getSnapshot()
    {
        std::shared_lock<std::shared_mutex> lock(read_mutex);
        return std::make_shared<Snapshot>(this, current, &read_mutex);
    }

public:
    VersionBase_t * base;
    VersionDelta_t placeholder_node;
    VersionDelta_t * current;

    mutable std::shared_mutex read_mutex;

protected:
    void appendVersion(VersionDelta_t * const v)
    {
        assert(v->ref_count == 0);
        assert(v != current);
        current = v;
        current->incrRefCount();

        // Append to linked list
        current->prev = placeholder_node.prev;
        current->next = &placeholder_node;
        current->prev->next = current;
        current->next->prev = current;
    }
};

} // namespace MVCC
} // namespace DB

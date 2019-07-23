#pragma once

#include <stdint.h>
#include <cassert>
#include <mutex>
#include <shared_mutex>

#include <IO/WriteHelpers.h>

namespace DB
{
namespace MVCC
{

/// Config
struct VersionSetConfig
{
    size_t compact_hint_delta_deletions = 5000;
    size_t compact_hint_delta_entries   = 200 * 1000;
};

/// Base type for VersionType of VersionSet
template <typename T>
struct MultiVersionCountable
{
public:
    std::atomic<uint32_t> ref_count;
    T *                   next;
    T *                   prev;

public:
    explicit MultiVersionCountable(T * self) : ref_count(0), next(self), prev(self) {}
    virtual ~MultiVersionCountable()
    {
        assert(ref_count == 0);

        // Remove from linked list
        prev->next = next;
        next->prev = prev;
    }

    void incrRefCount() { ++ref_count; }

    void decrRefCount(std::shared_mutex & mutex)
    {
        assert(ref_count >= 1);
        if (--ref_count == 0)
        {
            // in case two neighbor nodes remove from linked list
            std::unique_lock lock(mutex);
            delete this;
        }
    }

    // Not thread-safe, caller ensure.
    void decrRefCount()
    {
        assert(ref_count >= 1);
        if (--ref_count == 0)
        {
            delete this; // remove this node from version set
        }
    }
};

/// VersionSet -- Manage multiple versions
///
/// \tparam Version_t
///   member required:
///     Version_t::prev
///         -- previous version
///     Version_t::next
///         -- next version
///   functions required:
///     void Version_t::incrRefCount()
///         -- increase version's ref count
///         -- Note: must be thread safe
///     void Version_t::decrRefCount(std::shared_mutex &mutex)
///         -- decrease version's ref count. If version's ref count down to 0, it acquire unique_lock for mutex and then remove itself from version set
///
/// \tparam VersionEdit_t -- Changes between two version
/// \tparam Builder_t     -- Apply one or more VersionEdit_t to base version and build a new version
///   functions required:
///     Builder_t(Version_t *base)
///         -- Create a builder base on version `base`
///     void Builder_t::apply(const VersionEdit_t &)
///         -- Apply edit to builder
///     Version_t* Builder_t::build()
///         -- Build new version
template <typename Version_t, typename VersionEdit_t, typename Builder_t>
class VersionSet
{
public:
    using BuilderType = Builder_t;

public:
    explicit VersionSet(const VersionSetConfig & config_ = VersionSetConfig()) : placeholder_node(), current(nullptr)
    {
        (void)config_; // just ignore config
        // append a init version to link
        appendVersion(new Version_t);
    }

    virtual ~VersionSet()
    {
        current->decrRefCount();
        assert(placeholder_node.next == &placeholder_node); // List must be empty
    }

    void restore(Version_t * const v)
    {
        std::unique_lock read_lock(read_mutex);
        appendVersion(v);
    }

    /// `apply` accept changes and append new version to version-list
    void apply(const VersionEdit_t & edit)
    {
        std::unique_lock read_lock(read_mutex);

        // apply edit on base
        Version_t * v = nullptr;
        {
            Builder_t builder(current);
            builder.apply(edit);
            v = builder.build();
        }

        appendVersion(v);
    }

    size_t size() const
    {
        std::unique_lock read_lock(read_mutex);
        size_t           sz = 0;
        for (Version_t * v = current; v != &placeholder_node; v = v->prev)
            sz += 1;
        return sz;
    }

    std::string toDebugStringUnlocked() const
    {
        std::string s;
        for (Version_t * v = placeholder_node.next; v != &placeholder_node; v = v->next)
        {
            if (!s.empty())
                s += "->";
            s += "{\"rc\":";
            s += DB::toString(uint32_t(v->ref_count));
            s += '}';
        }
        return s;
    }

public:
    /// A snapshot class for holding particular version
    class Snapshot
    {
    private:
        Version_t *         v;     // particular version
        std::shared_mutex * mutex; // mutex to be used when freeing version

    public:
        Snapshot(Version_t * version_, std::shared_mutex * mutex_) : v(version_), mutex(mutex_) { v->incrRefCount(); }
        ~Snapshot() { v->decrRefCount(*mutex); }

        const Version_t * version() const { return v; }

    public:
        // No copying allowed.
        Snapshot(const Snapshot &) = delete;
        Snapshot & operator=(const Snapshot &) = delete;
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version
    SnapshotPtr getSnapshot()
    {
        std::shared_lock<std::shared_mutex> lock(read_mutex);
        return std::make_shared<Snapshot>(current, &read_mutex);
    }

protected:
    Version_t   placeholder_node; // Head of circular double-linked list of all versions
    Version_t * current;          // current version; current == placeholder_node.prev

    mutable std::shared_mutex read_mutex;

protected:
    void appendVersion(Version_t * const v)
    {
        // Make "v" become "current"
        assert(v->ref_count == 0);
        assert(v != current);
        if (current != nullptr)
        {
            current->decrRefCount();
        }
        current = v;
        current->incrRefCount();

        // Append to linked list
        current->prev       = placeholder_node.prev;
        current->next       = &placeholder_node;
        current->prev->next = current;
        current->next->prev = current;
    }

public:
    // No copying allowed
    VersionSet(const VersionSet &) = delete;
    VersionSet & operator=(const VersionSet &) = delete;
};


} // namespace MVCC
} // namespace DB

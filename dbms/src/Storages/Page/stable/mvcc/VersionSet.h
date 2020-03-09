#pragma once

#include <IO/WriteHelpers.h>
#include <stdint.h>

#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <mutex>
#include <shared_mutex>

/* TODO: This abstraction above PageEntriesVersion seems to be redundant and 
make changes hard to apply. (template make something wried between VersionSet, 
PageEntriesVersionSet and PageEntriesBuilder).
Maybe we should make VersionSet and PageEntriesVersionSet into one class later.
And also VersionSetWithDelta and PageEntriesVersionSetWithDelta.
 */

namespace DB::stable
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
    uint32_t ref_count;
    T *      next;
    T *      prev;

public:
    explicit MultiVersionCountable(T * self) : ref_count(0), next(self), prev(self) {}
    virtual ~MultiVersionCountable()
    {
        assert(ref_count == 0);

        // Remove from linked list
        prev->next = next;
        next->prev = prev;
    }

    void increase(const std::unique_lock<std::shared_mutex> & lock)
    {
        (void)lock;
        ++ref_count;
    }

    void release(const std::unique_lock<std::shared_mutex> & lock)
    {
        (void)lock;
        assert(ref_count >= 1);
        if (--ref_count == 0)
        {
            // in case two neighbor nodes remove from linked list
            delete this;
        }
    }

    // Not thread-safe function. Only for VersionSet::Builder.

    // Not thread-safe, caller ensure.
    void increase() { ++ref_count; }

    // Not thread-safe, caller ensure.
    void release()
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
/// \tparam TVersion
///   member required:
///     TVersion::prev
///         -- previous version
///     TVersion::next
///         -- next version
///   functions required:
///     void TVersion::increase
///         -- increase version's ref count
///     void TVersion::release
///         -- decrease version's ref count. If version's ref count down to 0, it acquire unique_lock for mutex and then remove itself from version set
///
/// \tparam TVersionEdit -- Changes between two version
///
/// \tparam TBuilder     -- Apply one or more TVersionEdit to base version and build a new version
///   functions required:
///     TBuilder(Version_t *base)
///         -- Create a builder base on version `base`
///     void TBuilder::apply(const TVersionEdit &)
///         -- Apply edit to builder
///     Version_t* TBuilder::build()
///         -- Build new version
template <typename TVersion, typename TVersionEdit, typename TBuilder>
class VersionSet
{
public:
    using BuilderType = TBuilder;
    using VersionType = TVersion;
    using VersionPtr  = VersionType *;

public:
    explicit VersionSet(const VersionSetConfig & config_ = VersionSetConfig()) : placeholder_node(), current(nullptr)
    {
        (void)config_; // just ignore config
        // append a init version to link
        appendVersion(new VersionType, std::unique_lock(read_write_mutex));
    }

    virtual ~VersionSet()
    {
        // All versions of this VersionSet must be released before destructuring VersionSet.
        current->release(std::unique_lock<std::shared_mutex>(read_write_mutex));
        assert(placeholder_node.next == &placeholder_node); // All versions are removed. (List must be empty)
    }

    void restore(VersionPtr const v)
    {
        std::unique_lock lock(read_write_mutex);
        appendVersion(v, lock);
    }

    /// `apply` accept changes and append new version to version-list
    void apply(const TVersionEdit & edit)
    {
        std::unique_lock lock(read_write_mutex);

        // apply edit on base
        VersionPtr v = nullptr;
        {
            BuilderType builder(current);
            builder.apply(edit);
            v = builder.build();
        }

        appendVersion(v, lock);
    }

    size_t size() const
    {
        std::shared_lock lock(read_write_mutex);

        size_t sz = 0;
        for (VersionPtr v = current; v != &placeholder_node; v = v->prev)
            sz += 1;
        return sz;
    }

    std::string toDebugString() const
    {
        std::shared_lock lock(read_write_mutex);
        std::string      s;
        for (VersionPtr v = placeholder_node.next; v != &placeholder_node; v = v->next)
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
        VersionPtr          v;     // particular version
        std::shared_mutex * mutex; // mutex to be used when freeing version

    public:
        Snapshot(VersionPtr version_, std::shared_mutex * mutex_) : v(version_), mutex(mutex_)
        {
            std::unique_lock lock(*mutex);
            v->increase(lock);
        }
        ~Snapshot()
        {
            std::unique_lock lock(*mutex);
            v->release(lock);
        }

        VersionPtr version() const { return v; }

    public:
        // No copying allowed.
        Snapshot(const Snapshot &) = delete;
        Snapshot & operator=(const Snapshot &) = delete;
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    /// Create a snapshot for current version
    SnapshotPtr getSnapshot() { return std::make_shared<Snapshot>(current, &read_write_mutex); }

protected:
    VersionType placeholder_node; // Head of circular double-linked list of all versions
    VersionPtr  current;          // current version; current == placeholder_node.prev

    mutable std::shared_mutex read_write_mutex;

protected:
    void appendVersion(VersionPtr const v, const std::unique_lock<std::shared_mutex> & lock)
    {
        // Make "v" become "current"
        assert(v->ref_count == 0);
        assert(v != current);
        if (current != nullptr)
        {
            current->release(lock);
        }
        current = v;
        current->increase(lock);

        // Append to linked list
        current->prev       = placeholder_node.prev;
        current->next       = &placeholder_node;
        current->prev->next = current;
        current->next->prev = current;
    }

    std::unique_lock<std::shared_mutex> acquireForLock() const { return std::unique_lock<std::shared_mutex>(read_write_mutex); }

public:
    // No copying allowed
    VersionSet(const VersionSet &) = delete;
    VersionSet & operator=(const VersionSet &) = delete;
};


} // namespace MVCC
} // namespace DB::stable

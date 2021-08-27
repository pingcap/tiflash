#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
namespace DM
{
class DeltaIndex;
using DeltaIndexPtr = std::shared_ptr<DeltaIndex>;

static std::atomic_uint64_t NEXT_DELTA_INDEX_ID{0};

class DeltaIndex
{
private:
    // This id is only used as Key in LRUCache.
    const UInt64 id;

    DeltaTreePtr delta_tree;

    size_t placed_rows;
    size_t placed_deletes;

    mutable std::mutex mutex;

public:
    struct Update
    {
        size_t delete_ranges_offset;
        size_t rows_offset;
        // old index -> new index
        TupleRefs idx_mapping;

        Update(size_t delete_ranges_offset_, size_t rows_offset_, const IColumn::Permutation & sort_perm)
            : delete_ranges_offset(delete_ranges_offset_)
            , rows_offset(rows_offset_)
            , idx_mapping(sort_perm.size())
        {
            for (size_t pos = 0; pos < sort_perm.size(); ++pos)
                idx_mapping[sort_perm[pos]] = pos;
        }
    };
    using Updates = std::vector<Update>;

private:
    void applyUpdates(const Updates & updates)
    {
        for (auto & update : updates)
        {
            if (placed_rows <= update.rows_offset)
            {
                // Current index does not contain any inserts which go shuffled.
                break;
            }
            else if (placed_rows < update.rows_offset + update.idx_mapping.size())
            {
                // Current index contains part of inserts which go shuffled, they should be removed.
                delta_tree->removeInsertsStartFrom(update.rows_offset);
                placed_rows = update.rows_offset;
                break;
            }
            else
            {
                // Current index contains all inserts which go shuffled, let's update them directly.
                delta_tree->updateTupleId(update.idx_mapping, update.rows_offset);
            }
        }
    }

    DeltaIndexPtr tryCloneInner(size_t placed_deletes_limit, const Updates * updates = nullptr)
    {
        DeltaTreePtr delta_tree_copy;
        size_t placed_rows_copy = 0;
        size_t placed_deletes_copy = 0;
        // Make sure the delta index do not place more deletes than `placed_deletes_limit`.
        // Because delete ranges can break MVCC view.
        {
            std::scoped_lock lock(mutex);
            // Safe to reuse the copy of the existing DeltaIndex
            if (placed_deletes <= placed_deletes_limit)
            {
                delta_tree_copy = delta_tree;
                placed_rows_copy = placed_rows;
                placed_deletes_copy = placed_deletes;
            }
        }

        if (delta_tree_copy)
        {
            auto new_delta_tree = std::make_shared<DefaultDeltaTree>(*delta_tree_copy);
            auto new_index = std::make_shared<DeltaIndex>(new_delta_tree, placed_rows_copy, placed_deletes_copy);
            // try to do some updates before return it if need
            if (updates)
                new_index->applyUpdates(*updates);
            return new_index;
        }
        else
        {
            // Otherwise, create an empty new DeltaIndex.
            return std::make_shared<DeltaIndex>();
        }
    }

public:
    DeltaIndex()
        : id(++NEXT_DELTA_INDEX_ID)
        , delta_tree(std::make_shared<DefaultDeltaTree>())
        , placed_rows(0)
        , placed_deletes(0)
    {}

    DeltaIndex(const DeltaTreePtr & delta_tree_, size_t placed_rows_, size_t placed_deletes_)
        : id(++NEXT_DELTA_INDEX_ID)
        , delta_tree(delta_tree_)
        , placed_rows(placed_rows_)
        , placed_deletes(placed_deletes_)
    {
    }

    /// Note that we don't swap the id.
    void swap(DeltaIndex & other)
    {
        std::scoped_lock lock(mutex, other.mutex);
        delta_tree.swap(other.delta_tree);
        std::swap(placed_rows, other.placed_rows);
        std::swap(placed_deletes, other.placed_deletes);
    }

    String toString()
    {
        std::stringstream s;
        s << "{placed rows:" << placed_rows << ", deletes:" << placed_deletes << ", delta tree: " << delta_tree->numEntries() << "|"
          << delta_tree->numInserts() << "|" << delta_tree->numDeletes() << "}";
        return s.str();
    }

    UInt64 getId() const { return id; }

    size_t getBytes() const
    {
        std::scoped_lock lock(mutex);
        return delta_tree->getBytes();
    }

    std::pair<size_t, size_t> getPlacedStatus()
    {
        std::scoped_lock lock(mutex);
        return {placed_rows, placed_deletes};
    }

    DeltaTreePtr getDeltaTree()
    {
        std::scoped_lock lock(mutex);
        return delta_tree;
    }

    void update(const DeltaTreePtr & delta_tree_, size_t placed_rows_, size_t placed_deletes_)
    {
        std::scoped_lock lock(mutex);
        delta_tree = delta_tree_;
        placed_rows = placed_rows_;
        placed_deletes = placed_deletes_;
    }

    bool updateIfAdvanced(const DeltaIndex & maybe_advanced)
    {
        std::scoped_lock lock(mutex);

        if ((maybe_advanced.placed_rows >= placed_rows && maybe_advanced.placed_deletes >= placed_deletes)
            && !(maybe_advanced.placed_rows == placed_rows && maybe_advanced.placed_deletes == placed_deletes))
        {
            delta_tree = maybe_advanced.delta_tree;
            placed_rows = maybe_advanced.placed_rows;
            placed_deletes = maybe_advanced.placed_deletes;
            return true;
        }
        return false;
    }

    DeltaIndexPtr tryClone(size_t /*rows*/, size_t deletes) { return tryCloneInner(deletes); }

    DeltaIndexPtr cloneWithUpdates(const Updates & updates)
    {
        if (unlikely(updates.empty()))
            throw Exception("Unexpected empty updates");

        return tryCloneInner(updates.front().delete_ranges_offset, &updates);
    }
};

} // namespace DM
} // namespace DB
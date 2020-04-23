#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>

namespace DB
{
namespace DM
{

class DeltaIndex;
using DeltaIndexPtr = std::shared_ptr<DeltaIndex>;

class DeltaIndex
{
private:
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
            : delete_ranges_offset(delete_ranges_offset_), rows_offset(rows_offset_), idx_mapping(sort_perm.size())
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

public:
    DeltaIndex() : delta_tree(std::make_shared<DefaultDeltaTree>()), placed_rows(0), placed_deletes(0) {}
    DeltaIndex(const DeltaIndex & o)
    {
        DeltaTreePtr delta_tree_copy;
        {
            std::scoped_lock lock(o.mutex);
            delta_tree_copy = o.delta_tree;
            placed_rows     = o.placed_rows;
            placed_deletes  = o.placed_deletes;
        }
        delta_tree = std::make_shared<DefaultDeltaTree>(*delta_tree_copy);
    }
    String toString()
    {
        std::stringstream s;
        s << "{placed rows:" << placed_rows << ", deletes:" << placed_deletes << ", delta tree: " << delta_tree->numEntries() << "|"
          << delta_tree->numInserts() << "|" << delta_tree->numDeletes() << "}";
        return s.str();
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
        delta_tree     = delta_tree_;
        placed_rows    = placed_rows_;
        placed_deletes = placed_deletes_;
    }

    bool updateIfAdvanced(const DeltaIndex & maybe_advanced)
    {
        std::scoped_lock lock(mutex);

        if ((maybe_advanced.placed_rows >= placed_rows && maybe_advanced.placed_deletes >= placed_deletes)
            && !(maybe_advanced.placed_rows == placed_rows && maybe_advanced.placed_deletes == placed_deletes))
        {
            delta_tree     = maybe_advanced.delta_tree;
            placed_rows    = maybe_advanced.placed_rows;
            placed_deletes = maybe_advanced.placed_deletes;
            return true;
        }
        return false;
    }

    DeltaIndexPtr tryClone(size_t /*rows*/, size_t deletes)
    {
        // Delete ranges can break MVCC view.
        {
            std::scoped_lock lock(mutex);

            if (placed_deletes > deletes)
                return std::make_shared<DeltaIndex>();
        }
        // Otherwise, clone it.
        return std::make_shared<DeltaIndex>(*this);
    }

    DeltaIndexPtr cloneWithUpdates(const Updates & updates)
    {
        if (unlikely(updates.empty()))
            throw Exception("Unexpected empty updates");

        {
            std::scoped_lock lock(mutex);
            // If inserts shuffled before delete range, the old index cannot used any more.
            if (placed_deletes > updates.front().delete_ranges_offset)
                return std::make_shared<DeltaIndex>();
        }

        // Otherwise clone a new index, and do some updates.
        auto new_index = std::make_shared<DeltaIndex>(*this);
        new_index->applyUpdates(updates);
        return new_index;
    }
};

} // namespace DM
} // namespace DB
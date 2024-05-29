// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/Page/PageDefinesBase.h>

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

    DeltaIndexPtr tryCloneInner(size_t rows_limit, size_t placed_deletes_limit, const Updates * updates = nullptr)
    {
        DeltaTreePtr delta_tree_copy;
        size_t placed_rows_copy = 0;
        size_t placed_deletes_copy = 0;
        {
            std::scoped_lock lock(mutex);
            // Make sure the MVCC view will not be broken by the mismatch of delta index and snapshot:
            // - First, make sure the delta index do not place more deletes than `placed_deletes_limit`.
            // - Second, make sure the snapshot includes all duplicated tuples in the delta index.
            if (placed_deletes <= placed_deletes_limit && delta_tree->maxDupTupleID() < static_cast<Int64>(rows_limit))
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
        std::scoped_lock lock(mutex);
        return fmt::format("<placed_rows={} placed_deletes={} tree_entries={} tree_inserts={} tree_deletes={}>",
                           placed_rows,
                           placed_deletes,
                           delta_tree->numEntries(),
                           delta_tree->numInserts(),
                           delta_tree->numDeletes());
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

        if ((maybe_advanced.placed_rows >= placed_rows && maybe_advanced.placed_deletes >= placed_deletes) // advance
            // not excatly the same
            && (maybe_advanced.placed_rows != placed_rows || maybe_advanced.placed_deletes != placed_deletes))
        {
            delta_tree = maybe_advanced.delta_tree;
            placed_rows = maybe_advanced.placed_rows;
            placed_deletes = maybe_advanced.placed_deletes;
            return true;
        }
        return false;
    }

    /**
     * Try to get a clone of current instance.
     * Return an empty DeltaIndex if `deletes < this->placed_deletes` because the advanced delta-index will break
     * the MVCC view.
     */
    DeltaIndexPtr tryClone(size_t rows, size_t deletes) { return tryCloneInner(rows, deletes); }

    DeltaIndexPtr cloneWithUpdates(const Updates & updates)
    {
        RUNTIME_CHECK_MSG(!updates.empty(), "Unexpected empty updates");
        return tryCloneInner(updates.front().rows_offset, updates.front().delete_ranges_offset, &updates);
    }
};

} // namespace DM
} // namespace DB
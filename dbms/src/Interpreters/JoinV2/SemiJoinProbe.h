// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoinProbe.h>

namespace DB
{

/// A reusable, index‑based doubly‑linked circular list for managing semi join pending probe rows.
/// Supports O(1) append/remove by index.
class SemiJoinPendingProbeList
{
public:
    using IndexType = UInt32;

    struct PendingProbeRow
    {
        RowPtr build_row_ptr;
        bool has_null_eq_from_in;
        UInt32 pace;
        /// Embedded list indexes
        IndexType prev_idx;
        IndexType next_idx;
    };

    class Iterator
    {
    public:
        Iterator(SemiJoinPendingProbeList & list, IndexType idx)
            : list(list)
            , idx(idx)
        {}

        inline IndexType getIndex() const { return idx; }

        PendingProbeRow & operator*() const { return list.probe_rows[idx]; }
        PendingProbeRow * operator->() const { return &list.probe_rows[idx]; }

        Iterator & operator++()
        {
            idx = list.probe_rows[idx].next_idx;
            return *this;
        }

        bool operator!=(const Iterator & other) const { return idx != other.idx; }

    private:
        SemiJoinPendingProbeList & list;
        IndexType idx;
    };

    SemiJoinPendingProbeList() = default;

    /// After reset(n), it holds n entries plus a sentinel at index n.
    void reset(size_t n)
    {
        RUNTIME_CHECK(n <= UINT32_MAX);
        probe_rows.resize(n + 1);
        sentinel_idx = static_cast<IndexType>(n);
        // Sentinel circular self-loop
        probe_rows[sentinel_idx].prev_idx = sentinel_idx;
        probe_rows[sentinel_idx].next_idx = sentinel_idx;

#ifndef NDEBUG
        // reset should be called after all slots are removed
        assert(slot_count == 0);
        // Isolate all slots
        for (IndexType i = 0; i < n; ++i)
        {
            probe_rows[i].prev_idx = i;
            probe_rows[i].next_idx = i;
        }
#endif
    }

    /// Returns the number of usable slots in the list (excluding the sentinel).
    inline size_t slotSize() const { return probe_rows.size() - 1; }

    /// Append an existing slot by index at the tail (before sentinel).
    inline void append(IndexType idx)
    {
#ifndef NDEBUG
        assert(idx < slotSize());
        assert(probe_rows[idx].prev_idx == idx && probe_rows[idx].next_idx == idx);
        ++slot_count;
#endif
        IndexType tail = probe_rows[sentinel_idx].prev_idx;
        probe_rows[tail].next_idx = idx;
        probe_rows[idx].prev_idx = tail;
        probe_rows[idx].next_idx = sentinel_idx;
        probe_rows[sentinel_idx].prev_idx = idx;
    }

    /// Remove a slot by index from the list.
    inline void remove(IndexType idx)
    {
#ifndef NDEBUG
        assert(idx < slotSize());
        assert(probe_rows[idx].prev_idx != idx && probe_rows[idx].next_idx != idx);
        assert(slot_count > 0);
        --slot_count;
#endif
        IndexType prev = probe_rows[idx].prev_idx;
        IndexType next = probe_rows[idx].next_idx;
        probe_rows[prev].next_idx = next;
        probe_rows[next].prev_idx = prev;
    }

    Iterator begin() { return Iterator(*this, probe_rows[sentinel_idx].next_idx); }
    Iterator end() { return Iterator(*this, sentinel_idx); }

private:
    PaddedPODArray<PendingProbeRow> probe_rows;
    IndexType sentinel_idx = 0;
#ifndef NDEBUG
    size_t slot_count = 0;
#endif
};

template <ASTTableJoin::Kind kind>
struct SemiJoinProbeAdder;

class HashJoin;
class SemiJoinProbeHelper : public JoinProbeHelperUtil
{
public:
    explicit SemiJoinProbeHelper(const HashJoin * join);

    static bool isSupported(ASTTableJoin::Kind kind, bool has_other_condition);

    Block probe(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

private:
    template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer>
    Block probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

    template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer, bool fill_list>
    void NO_INLINE probeFillColumns(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);

    template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer, bool fill_list>
    void NO_INLINE
    probeFillColumnsPrefetch(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);

private:
    template <ASTTableJoin::Kind kind>
    friend struct SemiJoinProbeAdder;

    using FuncType = Block (SemiJoinProbeHelper::*)(JoinProbeContext &, JoinProbeWorkerData &);
    FuncType func_ptr_has_null = nullptr;
    FuncType func_ptr_no_null = nullptr;

    const HashJoin * join;
    const HashJoinPointerTable & pointer_table;
};


} // namespace DB

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

/// A index‑based doubly‑linked circular list for managing semi join pending probe rows.
/// After reset(N), it holds N entries plus a sentinel at index 0, and supports O(1) insert/remove by index.
class SemiJoinPendingProbeList
{
public:
    using index_t = UInt32;

    struct PendingProbeRow
    {
        RowPtr build_row_ptr;
        UInt32 pace;
        /// Embedded list pointers
        UInt32 prev_node;
        UInt32 next_node;
    };

    class Iterator
    {
    public:
        Iterator(SemiJoinPendingProbeList * list, index_t idx)
            : list(list)
            , idx(idx)
        {}

        PendingProbeRow & operator*() const { return list->probe_rows[idx]; }
        PendingProbeRow * operator->() const { return &list->probe_rows[idx]; }

        Iterator & operator++()
        {
            idx = list->probe_rows[idx].next_node;
            return *this;
        }

        bool operator!=(const Iterator & other) const { return idx != other.idx; }

    private:
        SemiJoinPendingProbeList * list;
        index_t idx;
    };

    SemiJoinPendingProbeList() = default;

    void reset(size_t n)
    {
        RUNTIME_CHECK(n <= UINT32_MAX);
        probe_rows.clear();
        probe_rows.resize(n + 1);
        // Sentinel circular self-loop
        probe_rows[0].next_node = 0;
        probe_rows[0].prev_node = 0;
    }

    inline size_t size() { return probe_rows.size() - 1; }

    /// Append an existing slot by index at the tail (before sentinel)
    inline void append(index_t idx)
    {
        index_t tail = probe_rows[0].prev_node;
        probe_rows[tail].next_node = idx;
        probe_rows[idx].prev_node = tail;
        probe_rows[idx].next_node = 0;
        probe_rows[0].prev_node = idx;
    }

    /// Remove a slot by index from the list
    inline void remove(index_t idx)
    {
        index_t prev = probe_rows[idx].prev_node;
        index_t next = probe_rows[idx].next_node;
        probe_rows[prev].next_node = next;
        probe_rows[next].prev_node = prev;
    }

    Iterator begin() { return Iterator(this, probe_rows[0].next_node); }
    Iterator end() { return Iterator(this, 0); }

    PendingProbeRow & operator[](index_t idx) { return probe_rows[idx]; }
    const PendingProbeRow & operator[](index_t idx) const { return probe_rows[idx]; }

private:
    PaddedPODArray<PendingProbeRow> probe_rows;
};

class HashJoin;
class SemiJoinProbeHelper : public JoinProbeHelperUtil
{
public:
    explicit SemiJoinProbeHelper(const HashJoin * join);

    Block probe(JoinProbeContext & context, JoinProbeWorkerData & wd);

private:
    template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer>
    Block probeImpl(JoinProbeContext & context, JoinProbeWorkerData & wd);

private:
    using FuncType = Block (SemiJoinProbeHelper::*)(JoinProbeContext &, JoinProbeWorkerData &);
    FuncType func_ptr_has_null = nullptr;
    FuncType func_ptr_no_null = nullptr;

    const HashJoin * join;
    const HashJoinPointerTable & pointer_table;
};


} // namespace DB

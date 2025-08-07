// Copyright 2025 PingCAP, Inc.
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

#include <Common/PODArray.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinRowLayout.h>

namespace DB
{

class ISemiJoinProbeList
{
public:
    virtual ~ISemiJoinProbeList() = default;
    virtual void reset(size_t n) = 0;
    virtual size_t activeSlots() const = 0;
};

/// A reusable, index‑based, doubly‑linked circular list for managing semi join pending probe rows.
/// Supports O(1) append/remove by index.
template <typename KeyGetter>
class SemiJoinProbeList final : public ISemiJoinProbeList
{
public:
    using IndexType = UInt32;

    struct ProbeRow
    {
        using KeyGetterType = typename KeyGetter::Type;
        using KeyType = typename KeyGetterType::KeyType;
        using HashValueType = typename KeyGetter::HashValueType;

        RowPtr build_row_ptr;
        bool has_null_eq_from_in;
        UInt16 pace;
        HashValueType hash;
        KeyType key;
    };

    class Iterator
    {
    public:
        Iterator(SemiJoinProbeList & list, IndexType idx)
            : list(list)
            , idx(idx)
        {}

        Iterator(const Iterator & other) = default;
        Iterator & operator=(const Iterator & other)
        {
            assert(&list == &other.list);
            idx = other.idx;
            return *this;
        }

        inline IndexType getIndex() const { return idx; }

        ProbeRow & operator*() const { return list.probe_rows[idx]; }
        ProbeRow * operator->() const { return &list.probe_rows[idx]; }

        Iterator & operator++()
        {
            idx = list.probe_rows[idx].next_idx;
            return *this;
        }

        bool operator!=(const Iterator & other) const
        {
            assert(&list == &other.list);
            return idx != other.idx;
        }

    private:
        SemiJoinProbeList & list;
        IndexType idx;
    };

    SemiJoinProbeList() = default;

    /// After reset(n), it holds n entries plus a sentinel at index n.
    void reset(size_t n) override
    {
        // reset should be called after all active slots are removed
        RUNTIME_CHECK(active_count == 0);
        RUNTIME_CHECK(n <= UINT32_MAX);
        probe_rows.resize(n + 1);
        sentinel_idx = static_cast<IndexType>(n);
        // Initialize sentinel self-loop
        probe_rows[sentinel_idx].prev_idx = sentinel_idx;
        probe_rows[sentinel_idx].next_idx = sentinel_idx;
        active_rows_in_list.clear();
        active_rows_in_list.resize_fill_zero(n);
    }

    /// Returns the number of usable slots in the list (excluding the sentinel).
    inline size_t slotCapacity() const { return probe_rows.size() - 1; }

    size_t activeSlots() const override { return active_count; }

    /// Returns true if the slot is currently linked in the list.
    inline bool contains(IndexType idx)
    {
        assert(idx < slotCapacity());
        return active_rows_in_list[idx];
    }

    /// Append an existing slot by index at the tail (before sentinel).
    inline void append(IndexType idx)
    {
        assert(idx < slotCapacity());
        assert(!contains(idx));
        active_rows_in_list[idx] = true;
        ++active_count;
        auto tail = probe_rows[sentinel_idx].prev_idx;
        probe_rows[tail].next_idx = idx;
        probe_rows[idx].prev_idx = tail;
        probe_rows[idx].next_idx = sentinel_idx;
        probe_rows[sentinel_idx].prev_idx = idx;
    }

    /// Remove a slot by index from the list.
    inline void remove(IndexType idx)
    {
        assert(idx < slotCapacity());
        assert(contains(idx));
        active_rows_in_list[idx] = false;
        assert(active_count > 0);
        --active_count;
        auto prev = probe_rows[idx].prev_idx;
        auto next = probe_rows[idx].next_idx;
        probe_rows[prev].next_idx = next;
        probe_rows[next].prev_idx = prev;
    }

    /// Remove a slot by iterator.
    inline Iterator remove(Iterator iter)
    {
        auto idx = iter.getIndex();
        ++iter;
        remove(idx);
        return iter;
    }

    Iterator begin() { return Iterator(*this, probe_rows[sentinel_idx].next_idx); }
    Iterator end() { return Iterator(*this, sentinel_idx); }

    ProbeRow & at(IndexType idx)
    {
        assert(idx < slotCapacity());
        return probe_rows[idx];
    }
    const ProbeRow & at(IndexType idx) const
    {
        assert(idx < slotCapacity());
        return probe_rows[idx];
    }

private:
    struct WrapProbeRow : ProbeRow
    {
        /// Embedded list indexes
        IndexType prev_idx;
        IndexType next_idx;
    };

    PaddedPODArray<WrapProbeRow> probe_rows;
    PaddedPODArray<UInt8> active_rows_in_list;
    IndexType sentinel_idx = 0;
    size_t active_count = 0;
};

std::unique_ptr<ISemiJoinProbeList> createSemiJoinProbeList(HashJoinKeyMethod method);

} // namespace DB

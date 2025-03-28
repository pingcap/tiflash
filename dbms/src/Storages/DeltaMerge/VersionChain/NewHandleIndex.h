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

#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#include <absl/container/btree_map.h>
#include <absl/hash/hash.h>
#pragma clang diagnostic pop

namespace DB::DM
{
namespace tests
{
class NewHandleIndexTest;
}

// NewHandleIndex maintains the **newly inserted** records in the delta: handle -> row_id.
// In order to save memory, it stores the hash value of the handle instead of the handle itself.
template <ExtraHandleType HandleType, typename Hash = absl::Hash<typename ExtraHandleRefType<HandleType>::type>>
class NewHandleIndex
{
public:
    using HandleRefType = typename ExtraHandleRefType<HandleType>::type;

    // Different handles may have hash collisions, so during the lookup process, after finding a matching hash value,
    // it is necessary to use the delta_reader to read the actual data for verification.
    std::optional<RowID> find(HandleRefType handle, DeltaValueReader & delta_reader, const UInt32 stable_rows) const
    {
        const auto hash_value = hasher(handle);
        const auto [start, end] = handle_to_row_id.equal_range(hash_value);
        if (start == end)
            return {};

        MutableColumns mut_cols(1);
        mut_cols[0] = createHandleColumn();
        mut_cols[0]->reserve(std::distance(start, end));
        size_t total_read_rows = 0;
        for (auto itr = start; itr != end; ++itr)
        {
            // TODO: Maybe we can support readRows in batch.
            // But hash collision is rare.
            // And the distribution of these conflicting values is typically scattered.
            // The benefits should be quite small, so it's not a high priority.
            const auto read_rows = delta_reader.readRows(
                mut_cols,
                /*offset*/ itr->second - stable_rows,
                /*limit*/ 1,
                /*range*/ nullptr);
            RUNTIME_CHECK(read_rows == 1, itr->second, stable_rows, read_rows);
            ++total_read_rows;
            ColumnView<HandleType> handles(*(mut_cols[0]));
            if (handles[total_read_rows - 1] == handle)
                return itr->second;
        }
        return {};
    }

    void insert(HandleRefType handle, RowID row_id)
    {
        std::ignore = handle_to_row_id.insert(std::pair{hasher(handle), row_id});
    }

    // The hash value of a handle cannot reflect the order of handles,
    // so data can only be deleted by traversing the entire structure.
    // First, collect all row_ids in the index and sort them.
    // Then, read the handles in batches from the delta, and check if the handles in the delete range.
    void deleteRange(const RowKeyRange & range, DeltaValueReader & delta_reader, const UInt32 stable_rows)
    {
        if (handle_to_row_id.empty())
            return;

        std::vector<RowID> row_ids;
        row_ids.reserve(handle_to_row_id.size());
        for (auto [h, row_id] : handle_to_row_id)
            row_ids.push_back(row_id - stable_rows);

        std::sort(row_ids.begin(), row_ids.end());

        absl::btree_multimap<UInt32, RowID> t;
        auto begin = row_ids.begin();
        auto end = row_ids.end();
        auto get_next_continuous_size = [](auto begin, auto end) {
            assert(begin != end);
            auto itr = std::next(begin);
            for (; itr != end && (*itr - *begin) == (itr - begin); ++itr) {}
            return itr - begin;
        };

        MutableColumns mut_cols(1);
        while (begin != end)
        {
            const auto size = get_next_continuous_size(begin, end);
            mut_cols[0] = createHandleColumn();
            mut_cols[0]->reserve(size);
            const auto read_rows
                = delta_reader.readRows(mut_cols, /*offset*/ *begin, /*limit*/ size, /*range*/ nullptr);
            RUNTIME_CHECK(std::cmp_equal(read_rows, size), *begin, size, read_rows);
            ColumnView<HandleType> handles(*(mut_cols[0]));
            for (size_t i = 0; i < read_rows; ++i)
            {
                auto h = handles[i];
                if (inRowKeyRange(range, h))
                    continue; // deleted

                std::ignore = t.insert(std::pair{hasher(h), *begin + i + stable_rows});
            }
            begin += size;
        }
        handle_to_row_id.swap(t);
    }

private:
    static MutableColumnPtr createHandleColumn()
    {
        if constexpr (std::is_same_v<HandleType, String>)
            return ColumnString::create();
        else
            return ColumnInt64::create();
    }

    Hash hasher;
    absl::btree_multimap<UInt32, RowID> handle_to_row_id;

    friend class tests::NewHandleIndexTest;
};


} // namespace DB::DM

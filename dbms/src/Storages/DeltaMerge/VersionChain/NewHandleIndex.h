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
#include <Storages/DeltaMerge/VersionChain/Common.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"

#include <absl/container/btree_map.h>
//#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

namespace DB::DM
{

template <typename T>
class NewHandleIndex
{
    static_assert(false, "Only support Int64 and String");
};

template <>
class NewHandleIndex<Int64>
{
public:
    std::optional<RowID> find(Int64 handle, DeltaValueReader & /*delta_reader*/, const UInt32 /*stable_rows*/) const
    {
        if (auto itr = handle_to_row_id.find(handle); itr != handle_to_row_id.end())
            return itr->second;
        return {};
    }

    void insert(Int64 handle, RowID row_id)
    {
        auto [itr, inserted] = handle_to_row_id.try_emplace(handle, row_id);
        RUNTIME_CHECK_MSG(
            inserted,
            "Insert failed! handle={}, row_id={}, already exist row_id={}",
            handle,
            row_id,
            itr->second);
    }

    void deleteRange(const RowKeyRange & range, DeltaValueReader & /*delta_reader*/, const UInt32 /*stable_rows*/)
    {
        auto itr = handle_to_row_id.lower_bound(range.start.int_value);
        while (itr != handle_to_row_id.end() && itr->first < range.end.int_value)
            itr = handle_to_row_id.erase(itr);
    }

private:
    absl::btree_map<Int64, RowID> handle_to_row_id;
};

template <>
class NewHandleIndex<String>
{
public:
    std::optional<RowID> find(std::string_view handle, DeltaValueReader & delta_reader, const UInt32 stable_rows) const
    {
        const auto hash_value = hasher(handle);
        const auto [start, end] = handle_to_row_id.equal_range(hash_value);
        for (auto itr = start; itr != end; ++itr)
        {
            MutableColumns mut_cols(1);
            mut_cols[0] = ColumnString::create();
            const auto read_rows
                = delta_reader.readRows(mut_cols, /*offset*/ itr->second - stable_rows, /*limit*/ 1, /*range*/ nullptr);
            RUNTIME_CHECK(read_rows == 1, itr->second, stable_rows, read_rows);
            if (mut_cols[0]->getDataAt(0) == handle)
                return itr->second;
        }
        return {};
    }

    void insert(std::string_view handle, RowID row_id)
    {
        const auto hash_value = hasher(handle);
        [[maybe_unused]] auto itr = handle_to_row_id.insert(std::pair{hash_value, row_id});
    }

    void deleteRange(const RowKeyRange & range, DeltaValueReader & delta_reader, const UInt32 stable_rows)
    {
        if (handle_to_row_id.empty())
            return;

        std::vector<RowID> row_ids;
        row_ids.reserve(handle_to_row_id.size());
        for (auto [h, row_id] : handle_to_row_id)
            row_ids.push_back(row_id - stable_rows);

        std::sort(row_ids.begin(), row_ids.end());

        absl::btree_multimap<Int64, RowID> t;
        auto begin = row_ids.begin();
        auto end = row_ids.end();

        auto get_next_continuous_size = [](auto begin, auto end) {
            RUNTIME_CHECK(begin != end);
            auto itr = std::next(begin);
            for (; itr != end && (*itr - *begin) == (itr - begin); ++itr) {}
            return itr - begin;
        };

        while (begin != end)
        {
            const auto size = get_next_continuous_size(begin, end);
            MutableColumns mut_cols(1);
            mut_cols[0] = ColumnString::create();
            const auto read_rows
                = delta_reader.readRows(mut_cols, /*offset*/ *begin, /*limit*/ size, /*range*/ nullptr);
            RUNTIME_CHECK(std::cmp_equal(read_rows, size), *begin, size, read_rows);

            ColumnView<String> handles(*(mut_cols[0]));
            for (size_t i = 0; i < read_rows; ++i)
            {
                auto h = handles[i];
                if (inRowKeyRange(range, h))
                    continue; // deleted

                const auto hash_value = hasher(h);
                [[maybe_unused]] auto itr = t.insert(std::pair{hash_value, *begin + i + stable_rows});
            }

            begin += size;
        }
        handle_to_row_id.swap(t);
    }

private:
    absl::Hash<std::string_view> hasher;
    absl::btree_multimap<Int64, RowID> handle_to_row_id;
};


} // namespace DB::DM

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
    std::optional<RowID> find(Int64 handle) const
    {
        if (auto itr = handle_to_row_id.find(handle); itr != handle_to_row_id.end())
            return itr->second;
        return {};
    }

    void insert(Int64 handle, RowID row_id)
    {
        auto [itr, inserted] = handle_to_row_id.try_emplace(handle, row_id);
        RUNTIME_CHECK_MSG(inserted, "Insert failed! handle={}, row_id={}, already exist row_id={}", handle, row_id, itr->second);
    }

    void deleteRange(const RowKeyRange & range)
    {
        auto [start, end] = convertRowKeyRange(range);
        auto itr = handle_to_row_id->lower_bound(start);
        while (itr != handle_to_row_id->end() && itr->first < end)
            itr = handle_to_row_id->erase(itr);
    }
private:
    absl::btree_map<Int64, RowID> handle_to_row_id;
};

template <>
class NewHandleIndex<std::string_view>
{
public:
    std::optional<RowID> find(std::string_view handle) const
    {
        const auto hash_value = hasher(handle);
        const auto [start, end] = handle_to_row_id.equal_range(hash_value);
        for (auto itr = start; itr != end; ++itr)
        {
            // TODO: check value in itr->second euqal...
        }
        return {};
    }

    void insert(std::string_view handle, RowID row_id)
    {
        const auto hash_value = hasher(handle);
        auto [itr, inserted] = handle_to_row_id.insert(std::pair{hash_value, row_id});
        RUNTIME_CHECK_MSG(inserted, "Insert failed! handle={}, row_id={}, already exist row_id={}", handle, row_id, itr->second);
    }

    void deleteRange(const RowKeyRange & range)
    {
        auto [start, end] = convertRowKeyRange(range);
        auto itr = handle_to_row_id->lower_bound(start);
        while (itr != handle_to_row_id->end() && itr->first < end)
            itr = handle_to_row_id->erase(itr);
    }
private:
    absl::Hash<std::string_view> hasher;
    absl::btree_multimap<Int64, RowID> handle_to_row_id;
};


}
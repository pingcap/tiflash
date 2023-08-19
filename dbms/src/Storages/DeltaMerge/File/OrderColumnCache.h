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

#include <Columns/IColumn.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

#include <unordered_map>

namespace DB
{
namespace DM
{
class OrderColumnCache
{
private:
    using ColumnMap = std::unordered_map<UInt64, ColumnPtr>;
    ColumnMap column_map;

public:
    ColumnPtr get(size_t pack_id, ColId col_id)
    {
        SipHash hash;
        hash.update(pack_id);
        hash.update(col_id);
        UInt64 key = hash.get64();
        auto it = column_map.find(key);
        if (it == column_map.end())
            return {};
        else
            return it->second;
    }

    void put(size_t pack_id, ColId col_id, const ColumnPtr & col)
    {
        SipHash hash;
        hash.update(pack_id);
        hash.update(col_id);
        UInt64 key = hash.get64();
        column_map.emplace(key, col);
    }
};

using OrderColumnCachePtr = std::shared_ptr<OrderColumnCache>;

} // namespace DM
} // namespace DB
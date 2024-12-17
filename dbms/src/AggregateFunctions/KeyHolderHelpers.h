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
#include <Common/HashTable/HashTableKeyHolder.h>

namespace DB
{
template <bool is_plain_column = false>
inline auto getKeyHolder(const IColumn & column, size_t row_num, Arena & arena)
{
    if constexpr (is_plain_column)
    {
        return ArenaKeyHolder{column.getDataAt(row_num), arena};
    }
    else
    {
        const char * begin = nullptr;
        StringRef serialized = column.serializeValueIntoArena(row_num, arena, begin);
        assert(serialized.data != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
inline void deserializeAndInsert(StringRef str, IColumn & data_to)
{
    if constexpr (is_plain_column)
        data_to.insertData(str.data, str.size);
    else
        data_to.deserializeAndInsertFromArena(str.data);
}

} // namespace DB

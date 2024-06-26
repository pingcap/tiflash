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

#include <vector>

namespace DB
{

constexpr size_t ROW_ALIGN = 4;

inline size_t alignRowSize(size_t size)
{
    return (size + ROW_ALIGN - 1) / ROW_ALIGN * ROW_ALIGN;
}

struct HashJoinRowSchema
{
    std::vector<size_t> key_column_indexes;
    /// The raw join key are the same as the original data.
    /// raw_key_column_index + is_nullable
    std::vector<std::pair<size_t, bool>> raw_key_column_indexes;
    /// other_column_index + is_fixed_size
    std::vector<std::pair<size_t, bool>> other_column_indexes;
    size_t key_column_fixed_size = 0;
    size_t other_column_fixed_size = 0;
};

} // namespace DB
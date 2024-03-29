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

#include <Storages/Page/PageUtil.h>

#include <random>

namespace DB::PageUtil
{
UInt32 randInt(UInt32 min, UInt32 max)
{
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<UInt32> distribution(min, max);
    return distribution(generator);
}

std::vector<size_t> getFieldSizes(const std::set<FieldOffsetInsidePage> & field_offsets, size_t data_size)
{
    if (field_offsets.empty())
        return {};

    std::vector<size_t> sizes;
    sizes.reserve(field_offsets.size());
    auto iter = field_offsets.begin();
    size_t prev_field_offset = iter->offset;
    ++iter;
    while (iter != field_offsets.end())
    {
        sizes.emplace_back(iter->offset - prev_field_offset);
        prev_field_offset = iter->offset;
        ++iter;
    }
    // the size of last field
    sizes.emplace_back(data_size - prev_field_offset);
    return sizes;
}

} // namespace DB::PageUtil

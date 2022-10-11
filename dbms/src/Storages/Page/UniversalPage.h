// Copyright 2022 PingCAP, Ltd.
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

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>

#include <map>
#include <set>
#include <unordered_map>


namespace DB
{

struct UniversalPage
{
public:
    explicit UniversalPage(const UniversalPageId & page_id_)
        : page_id(page_id_)
    {
    }

    UniversalPageId page_id;
    ByteBuffer data;
    MemHolder mem_holder;
    // Field offsets inside this page.
    std::set<FieldOffsetInsidePage> field_offsets;

public:
    inline bool isValid() const { return !page_id.empty(); }

    ByteBuffer getFieldData(size_t index) const
    {
        auto iter = field_offsets.find(FieldOffsetInsidePage(index));
        if (unlikely(iter == field_offsets.end()))
            throw Exception(fmt::format("Try to getFieldData with invalid field index [page_id={}] [field_index={}]", page_id, index),
                            ErrorCodes::LOGICAL_ERROR);

        PageFieldOffset beg = iter->offset;
        ++iter;
        PageFieldOffset end = (iter == field_offsets.end() ? data.size() : iter->offset);
        assert(beg <= data.size());
        assert(end <= data.size());
        return ByteBuffer(data.begin() + beg, data.begin() + end);
    }

    inline static PageFieldSizes fieldOffsetsToSizes(const PageFieldOffsetChecksums & field_offsets, size_t data_size)
    {
        PageFieldSizes field_size = {};

        auto it = field_offsets.begin();
        while (it != field_offsets.end())
        {
            PageFieldOffset beg = it->first;
            ++it;
            field_size.emplace_back(it == field_offsets.end() ? data_size - beg : it->first - beg);
        }
        return field_size;
    }

    size_t fieldSize() const
    {
        return field_offsets.size();
    }
};

using UniversalPages = std::vector<UniversalPageId>;
using UniversalPageMap = std::map<UniversalPageId, UniversalPage>;

} // namespace DB

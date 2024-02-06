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

#include <IO/Buffer/BufferBase.h>
#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefinesBase.h>

#include <map>
#include <set>

namespace DB
{
using MemHolder = std::shared_ptr<char>;
inline MemHolder createMemHolder(char * memory, const std::function<void(char *)> & free)
{
    return std::shared_ptr<char>(memory, free);
}

struct FieldOffsetInsidePage
{
    size_t index;
    size_t offset;

    FieldOffsetInsidePage(size_t index_) // NOLINT(google-explicit-constructor)
        : index(index_)
        , offset(0)
    {}
    FieldOffsetInsidePage(size_t index_, size_t offset_)
        : index(index_)
        , offset(offset_)
    {}

    bool operator<(const FieldOffsetInsidePage & rhs) const { return index < rhs.index; }
};

struct Page
{
public:
    static Page invalidPage()
    {
        Page page{INVALID_PAGE_U64_ID};
        page.is_valid = false;
        return page;
    }

    explicit Page(PageIdU64 page_id_)
        : page_id(page_id_)
        , is_valid(true)
    {}

    PageIdU64 page_id;
    std::string_view data;
    MemHolder mem_holder;
    // Field offsets inside this page.
    std::set<FieldOffsetInsidePage> field_offsets;

private:
    bool is_valid;

public:
    inline bool isValid() const { return is_valid; }

    std::string_view getFieldData(size_t index) const
    {
        auto iter = field_offsets.find(FieldOffsetInsidePage(index));
        RUNTIME_CHECK_MSG(
            iter != field_offsets.end(),
            "Try to getFieldData with invalid field index [page_id={}] [valid={}] [field_index={}]",
            page_id,
            is_valid,
            index);
        PageFieldOffset beg = iter->offset;
        ++iter;
        PageFieldOffset end = (iter == field_offsets.end() ? data.size() : iter->offset);
        assert(beg <= data.size());
        assert(end <= data.size());
        assert(end >= beg);
        return std::string_view(data.begin() + beg, end - beg);
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

    size_t fieldSize() const { return field_offsets.size(); }
};

using Pages = std::vector<Page>;
using PageMapU64 = std::map<PageIdU64, Page>;

// Indicate the page size && offset in PageFile.
struct PageEntry
{
public:
    // if file_id == 0, means it is invalid
    PageFileId file_id = 0; // PageFile id
    PageSize size = 0; // Page data's size
    UInt64 offset = 0; // Page data's offset in PageFile
    UInt64 tag = 0;
    UInt64 checksum = 0; // The checksum of whole page data
    UInt32 level = 0; // PageFile level
    UInt32 ref = 1; // for ref counting

    // The offset to the begining of specify field.
    PageFieldOffsetChecksums field_offsets{};

public:
    inline bool isValid() const { return file_id != 0; }
    inline bool isTombstone() const { return ref == 0; }

    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }

    String toDebugString() const
    {
        return fmt::format(
            "PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}, tag: {}, ref: {}, field_offsets_size: "
            "{}}}",
            file_id,
            offset,
            size,
            checksum,
            tag,
            ref,
            field_offsets.size());
    }

    size_t getFieldSize(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(
                "Try to getFieldData of PageEntry" + DB::toString(file_id) + " with invalid index: "
                    + DB::toString(index) + ", fields size: " + DB::toString(field_offsets.size()),
                ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return size - field_offsets.back().first;
        else
            return field_offsets[index + 1].first - field_offsets[index].first;
    }

    // Return field{index} offsets: [begin, end) of page data.
    std::pair<size_t, size_t> getFieldOffsets(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(
                fmt::format(
                    "Try to getFieldOffsets with invalid index [index={}] [fields_size={}]",
                    index,
                    field_offsets.size()),
                ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return {field_offsets.back().first, size};
        else
            return {field_offsets[index].first, field_offsets[index + 1].first};
    }

    bool operator==(const PageEntry & rhs) const
    {
        bool is_ok = file_id == rhs.file_id && size == rhs.size && offset == rhs.offset && tag == rhs.tag
            && checksum == rhs.checksum && level == rhs.level && ref == rhs.ref
            && field_offsets.size() == rhs.field_offsets.size();
        if (!is_ok)
            return is_ok;
        // compare the fields offsets
        for (size_t i = 0; i < field_offsets.size(); ++i)
        {
            if (field_offsets[i] != rhs.field_offsets[i])
                return false;
        }
        return true;
    }
};
using PageIdU64AndEntry = std::pair<PageIdU64, PageEntry>;
using PageIdU64AndEntries = std::vector<PageIdU64AndEntry>;
} // namespace DB

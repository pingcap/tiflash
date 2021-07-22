#pragma once

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefines.h>
#include <common/likely.h>

#include <map>
#include <set>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using MemHolder = std::shared_ptr<char>;
inline MemHolder createMemHolder(char * memory, const std::function<void(char *)> & free)
{
    return std::shared_ptr<char>(memory, free);
}

struct Page
{
public:
    struct FieldOffset
    {
        size_t index;
        size_t offset;

        FieldOffset(size_t index_) : index(index_), offset(0) {}
        FieldOffset(size_t index_, size_t offset_) : index(index_), offset(offset_) {}

        bool operator<(const FieldOffset & rhs) const { return index < rhs.index; }
    };

    PageId     page_id;
    ByteBuffer data;
    MemHolder  mem_holder;
    // Field offsets inside this page.
    std::set<FieldOffset> field_offsets;

public:
    ByteBuffer getFieldData(size_t index) const
    {
        auto iter = field_offsets.find(index);
        if (unlikely(iter == field_offsets.end()))
            throw Exception("Try to getFieldData of Page" + DB::toString(page_id) + " with invalid field index: " + DB::toString(index),
                            ErrorCodes::LOGICAL_ERROR);

        PageFieldOffset beg = iter->offset;
        ++iter;
        PageFieldOffset end = (iter == field_offsets.end() ? data.size() : iter->offset);
        assert(beg <= data.size());
        assert(end <= data.size());
        return ByteBuffer(data.begin() + beg, data.begin() + end);
    }

    size_t fieldSize() const { return field_offsets.size(); }
};
using Pages       = std::vector<Page>;
using PageMap     = std::map<PageId, Page>;
using PageHandler = std::function<void(PageId page_id, const Page &)>;

// Indicate the page size && offset in PageFile.
struct PageEntry
{
    // if file_id == 0, means it is invalid
    PageFileId file_id  = 0; // PageFile id
    PageSize   size     = 0; // Page data's size
    UInt64     offset   = 0; // Page data's offset in PageFile
    UInt64     tag      = 0;
    UInt64     checksum = 0; // The checksum of whole page data
    UInt32     level    = 0; // PageFile level
    UInt32     ref      = 1; // for ref counting

    // The offset to the begining of specify field.
    PageFieldOffsetChecksums field_offsets{};

public:
    inline bool               isValid() const { return file_id != 0; }
    inline bool               isTombstone() const { return ref == 0; }
    inline PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }

    inline size_t getFieldSize(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception("Try to getFieldData of PageEntry" + DB::toString(file_id) + " with invalid index: " + DB::toString(index)
                                + ", fields size: " + DB::toString(field_offsets.size()),
                            ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return size - field_offsets.back().first;
        else
            return field_offsets[index + 1].first - field_offsets[index].first;
    }

    // Return field{index} offsets: [begin, end) of page data.
    inline std::pair<size_t, size_t> getFieldOffsets(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception("Try to getFieldData with invalid index: " + DB::toString(index)
                                + ", fields size: " + DB::toString(field_offsets.size()),
                            ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return {field_offsets.back().first, size};
        else
            return {field_offsets[index].first, field_offsets[index + 1].first};
    }

    bool operator==(const PageEntry & rhs) const
    {
        bool isOk = file_id == rhs.file_id && size == rhs.size && offset == rhs.offset && tag == rhs.tag && checksum == rhs.checksum
            && level == rhs.level && ref == rhs.ref && field_offsets.size() == rhs.field_offsets.size();
        if (!isOk)
            return isOk;
        else
        {
            for (size_t i = 0; i < field_offsets.size(); ++i)
            {
                if (field_offsets[i] != rhs.field_offsets[i])
                    return false;
            }
            return true;
        }
    }
};

using PageIdAndEntry   = std::pair<PageId, PageEntry>;
using PageIdAndEntries = std::vector<PageIdAndEntry>;

} // namespace DB

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

#include <Common/Exception.h>
#include <Storages/Page/PageDefines.h>
#include <fmt/format.h>

namespace DB
{
namespace PS::V3
{
class PageEntryV3;
using PageEntryV3Ptr = std::shared_ptr<PageEntryV3>;
using PageEntriesV3 = std::vector<PageEntryV3Ptr>;
using PageIDAndEntryV3 = std::pair<PageIdV3Internal, PageEntryV3Ptr>;
using PageIDAndEntriesV3 = std::vector<PageIDAndEntryV3>;

class PageEntryV3
{
protected:
    PageEntryV3() {}
    virtual ~PageEntryV3() = default;

public:
    PageEntryV3(const PageEntryV3 &) = delete;
    PageEntryV3 & operator=(PageEntryV3 &&) = delete;
    PageEntryV3 & operator=(const PageEntryV3 &) = delete;

    /** Whether it is a tight instance or not. A tight instance has smaller memory consumption.*/
    virtual bool isTight() const = 0;
    /** The id of page data persisted in */
    virtual BlobFileId getFileId() const = 0;
    /** The size of page data */
    virtual PageSize getSize() const = 0;
    /** The extra align size of page data */
    virtual PageSize getPaddedSize() const = 0;
    /** Tag of the page. It is specified by users. */
    virtual UInt64 getTag() const = 0;
    /** The offset of page data in file */
    virtual BlobFileOffset getOffset() const = 0;
    /** Checksum of the whole page data */
    virtual UInt64 getCheckSum() const = 0;
    /** The offset to the beginning of each field, and the checksums of them */
    virtual const PageFieldOffsetChecksums & getFieldOffsets() const = 0;
    /** The actual size on disk, i.e. size + padded_size */
    inline UInt64 getDiskSize() const { return getSize() + getPaddedSize(); }

    inline bool isValid() const { return getFileId() != INVALID_BLOBFILE_ID; }

    size_t getFieldSize(size_t index) const;

    // Return field{index} offsets: [begin, end) of page data.
    std::pair<size_t, size_t> getFieldOffsets(size_t index) const;

    inline String toDebugString() const
    {
        return fmt::format("PageEntryV3{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}, tag: {}, field_offsets_size: {}}}",
                           getFileId(),
                           getOffset(),
                           getSize(),
                           getCheckSum(),
                           getTag(),
                           getFieldOffsets().size());
    }
};

class PageEntryV3Loose : public PageEntryV3
{
private:
    BlobFileId file_id;
    PageSize size;
    PageSize padded_size;
    UInt64 tag;
    BlobFileOffset offset;
    UInt64 checksum;

    PageFieldOffsetChecksums field_offsets;

public:
    PageEntryV3Loose(BlobFileId file_id_, //
                     PageSize size_,
                     PageSize padded_size_,
                     UInt64 tag_,
                     BlobFileOffset offset_,
                     UInt64 checksum_,
                     PageFieldOffsetChecksums && field_offsets_)
        : file_id(file_id_)
        , size(size_)
        , padded_size(padded_size_)
        , tag(tag_)
        , offset(offset_)
        , checksum(checksum_)
        , field_offsets(std::move(field_offsets_))
    {}

    ~PageEntryV3Loose() = default;

    bool isTight() const { return false; }
    BlobFileId getFileId() const { return file_id; }
    PageSize getSize() const { return size; }
    PageSize getPaddedSize() const { return padded_size; }
    UInt64 getTag() const { return tag; }
    BlobFileOffset getOffset() const { return offset; }
    UInt64 getCheckSum() const { return checksum; }
    const PageFieldOffsetChecksums & getFieldOffsets() const { return field_offsets; }
};

/** A page entry with smaller memory consumption */
class PageEntryV3Tight : public PageEntryV3
{
private:
    BlobFileIdTight file_id;
    PageSizeTight size;
    BlobFileOffsetTight offset;
    UInt64 checksum;

public:
    PageEntryV3Tight(BlobFileIdTight file_id_, //
                     PageSizeTight size_,
                     BlobFileOffsetTight offset_,
                     UInt64 checksum_)
        : file_id(file_id_)
        , size(size_)
        , offset(offset_)
        , checksum(checksum_)
    {}

    ~PageEntryV3Tight() = default;

    bool isTight() const { return true; }
    BlobFileId getFileId() const { return file_id; }
    PageSize getSize() const { return size; }
    PageSize getPaddedSize() const { return 0; }
    UInt64 getTag() const { return 0; }
    BlobFileOffset getOffset() const { return offset; }
    UInt64 getCheckSum() const { return checksum; }
    const PageFieldOffsetChecksums & getFieldOffsets() const
    {
        static PageFieldOffsetChecksums empty;
        return empty;
    }
};

PageEntryV3Ptr makePageEntry(BlobFileId file_id, //
                             PageSize size,
                             PageSize padded_size,
                             UInt64 tag,
                             BlobFileOffset offset,
                             UInt64 checksum,
                             PageFieldOffsetChecksums && field_offsets = {});

inline PageEntryV3Ptr makeInvalidPageEntry()
{
    return makePageEntry(INVALID_BLOBFILE_ID, 0, 0, 0, 0, 0);
}


} // namespace PS::V3
} // namespace DB

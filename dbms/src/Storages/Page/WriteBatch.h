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

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/Remote/RemoteDataInfo.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

enum class WriteBatchWriteType : UInt8
{
    DEL = 0,
    // Create / Update a page, will implicitly create a RefPage{id} -> Page{id}.
    PUT = 1,
    // Create a RefPage{ref_id} -> Page{id}
    REF = 2,
    // Create or update a Page. Now only used by GC.
    // Compare to `PUT`, this type won't create the RefPage{id} -> Page{id} by default.
    UPSERT = 3,
    // Create an external page.
    // In V2, it is the same as `PUT`; In V3, we treated it as a different type from `PUT`
    // to get its lifetime management correct.
    PUT_EXTERNAL = 4,

    PUT_REMOTE = 5,
};

class WriteBatch : private boost::noncopyable
{
public:
    using SequenceID = UInt64;

private:
    struct Write
    {
        WriteBatchWriteType type;
        PageId page_id;
        UInt64 tag;
        // Page's data and size
        ReadBufferPtr read_buffer;
        PageSize size;
        // RefPage's origin page
        PageId ori_page_id;
        // Fields' offset inside Page's data
        PageFieldOffsetChecksums offsets;

        /// The meta and data may not be the same PageFile, (read_buffer == nullptr)
        /// use `target_file_id`, `page_offset`, `page_checksum` to indicate where
        /// data is actually store in.
        /// Should only use by `UPSERT` now.

        UInt64 page_offset;
        UInt64 page_checksum;
        PageFileIdAndLevel target_file_id;

        std::optional<PS::V3::RemoteDataLocation> remote_data_location = std::nullopt;
    };
    using Writes = std::vector<Write>;

public:
#ifdef DBMS_PUBLIC_GTEST
    WriteBatch()
        : namespace_id(TEST_NAMESPACE_ID)
    {}
#endif
    explicit WriteBatch(NamespaceId namespace_id_)
        : namespace_id(namespace_id_)
    {}
    WriteBatch(WriteBatch && rhs)
        : writes(std::move(rhs.writes))
        , sequence(rhs.sequence)
        , namespace_id(rhs.namespace_id)
    {}

    void putPage(PageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size, const PageFieldSizes & data_sizes = {})
    {
        // Convert from data_sizes to the offset of each field
        PageFieldOffsetChecksums offsets;
        PageFieldOffset off = 0;
        for (auto data_sz : data_sizes)
        {
            offsets.emplace_back(off, 0);
            off += data_sz;
        }
        if (unlikely(!data_sizes.empty() && off != size))
        {
            throw Exception(fmt::format(
                                "Try to put Page with fields, but page size and fields total size not match "
                                "[page_id={}] [num_fields={}] [page_size={}] [all_fields_size={}]",
                                page_id,
                                data_sizes.size(),
                                size,
                                off),
                            ErrorCodes::LOGICAL_ERROR);
        }

        Write w{WriteBatchWriteType::PUT, page_id, tag, read_buffer, size, 0, std::move(offsets), 0, 0, {}};
        total_data_size += size;
        writes.emplace_back(std::move(w));
    }

    void putPage(PageId page_id, UInt64 tag, std::string_view data)
    {
        auto buffer_ptr = std::make_shared<ReadBufferFromOwnString>(data);
        putPage(page_id, tag, buffer_ptr, data.size());
    }

    void putRemotePage(PageId page_id, UInt64 tag, const PS::V3::RemoteDataLocation & data_location, PageFieldOffsetChecksums && offset_and_checksums = {})
    {
        Write w{WriteBatchWriteType::PUT_REMOTE, page_id, tag, nullptr, /* size */ 0, 0, std::move(offset_and_checksums), 0, 0, {}, data_location};
        writes.emplace_back(std::move(w));
    }

    void putExternal(PageId page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w{WriteBatchWriteType::PUT_EXTERNAL, page_id, tag, nullptr, 0, 0, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    // Upsert a page{page_id} and writer page's data to a new PageFile{file_id}.
    // Now it's used in DataCompactor to move page's data to new file.
    void upsertPage(PageId page_id,
                    UInt64 tag,
                    const PageFileIdAndLevel & file_id,
                    const ReadBufferPtr & read_buffer,
                    UInt32 size,
                    const PageFieldOffsetChecksums & offsets)
    {
        Write w{WriteBatchWriteType::UPSERT, page_id, tag, read_buffer, size, 0, offsets, 0, 0, file_id};
        writes.emplace_back(std::move(w));
        total_data_size += size;
    }

    // Upserting a page{page_id} to PageFile{file_id}. This type of upsert is a simple mark and
    // only used for checkpoint. That page will be overwritten by WriteBatch with larger sequence,
    // so we don't need to write page's data.
    void upsertPage(PageId page_id,
                    UInt64 tag,
                    const PageFileIdAndLevel & file_id,
                    UInt64 page_offset,
                    UInt32 size,
                    UInt64 page_checksum,
                    const PageFieldOffsetChecksums & offsets)
    {
        Write w{WriteBatchWriteType::UPSERT, page_id, tag, nullptr, size, 0, offsets, page_offset, page_checksum, file_id};
        writes.emplace_back(std::move(w));
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageId ref_id, PageId page_id)
    {
        Write w{WriteBatchWriteType::REF, ref_id, 0, nullptr, 0, page_id, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void delPage(PageId page_id)
    {
        Write w{WriteBatchWriteType::DEL, page_id, 0, nullptr, 0, 0, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    bool empty() const
    {
        return writes.empty();
    }

    const Writes & getWrites() const
    {
        return writes;
    }
    Writes & getWrites()
    {
        return writes;
    }

    size_t putWriteCount() const
    {
        size_t count = 0;
        for (const auto & w : writes)
            count += (w.type == WriteBatchWriteType::PUT);
        return count;
    }

    void swap(WriteBatch & o)
    {
        writes.swap(o.writes);
        std::swap(o.total_data_size, total_data_size);
        std::swap(o.sequence, sequence);
    }

    void copyWrite(const Write write)
    {
        writes.emplace_back(write);
    }

    void copyWrites(const Writes & writes_)
    {
        for (const auto & w : writes_)
        {
            copyWrite(w);
        }
    }

    void clear()
    {
        Writes tmp;
        writes.swap(tmp);
        sequence = 0;
        total_data_size = 0;
    }

    SequenceID getSequence() const
    {
        return sequence;
    }

    size_t getTotalDataSize() const
    {
        return total_data_size;
    }

    // `setSequence` should only called by internal method of PageStorage.
    void setSequence(SequenceID seq)
    {
        sequence = seq;
    }

    NamespaceId getNamespaceId() const
    {
        return namespace_id;
    }

    PageIdV3Internal getFullPageId(PageId id) const
    {
        return buildV3Id(namespace_id, id);
    }

    String toString() const
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.joinStr(
            writes.begin(),
            writes.end(),
            [this](const auto w, FmtBuffer & fb) {
                switch (w.type)
                {
                case WriteBatchWriteType::PUT:
                    fb.fmtAppend("{}.{}", namespace_id, w.page_id);
                    break;
                case WriteBatchWriteType::REF:
                    fb.fmtAppend("{}.{} > {}.{}", namespace_id, w.page_id, namespace_id, w.ori_page_id);
                    break;
                case WriteBatchWriteType::DEL:
                    fb.fmtAppend("X{}.{}", namespace_id, w.page_id);
                    break;
                case WriteBatchWriteType::UPSERT:
                    fb.fmtAppend("U{}.{}", namespace_id, w.page_id);
                    break;
                case WriteBatchWriteType::PUT_EXTERNAL:
                    fb.fmtAppend("E{}.{}", namespace_id, w.page_id);
                    break;
                default:
                    fb.fmtAppend("Unknow {}.{}", namespace_id, w.page_id);
                    break;
                };
            },
            ",");
        return fmt_buffer.toString();
    }

private:
    Writes writes;
    SequenceID sequence = 0;
    NamespaceId namespace_id;
    size_t total_data_size = 0;
};
} // namespace DB

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
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdTrait.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

inline UniversalPageId buildTableUniversalPageId(const String & prefix, NamespaceId ns_id, PageId id)
{
    WriteBufferFromOwnString buff;
    writeString(prefix, buff);
    UniversalPageIdFormat::encodeUInt64(ns_id, buff);
    writeString("_", buff);
    UniversalPageIdFormat::encodeUInt64(id, buff);
    return buff.releaseStr();
}

inline UniversalPageId buildTableUniversalPrefix(const String & prefix, NamespaceId ns_id)
{
    WriteBufferFromOwnString buff;
    writeString(prefix, buff);
    UniversalPageIdFormat::encodeUInt64(ns_id, buff);
    writeString("_", buff);
    return buff.releaseStr();
}

class UniversalWriteBatch : private boost::noncopyable
{
private:
    struct Write
    {
        WriteBatchWriteType type;
        UniversalPageId page_id;
        UInt64 tag;
        // Page's data and size
        ReadBufferPtr read_buffer;
        PageSize size;
        // RefPage's origin page
        UniversalPageId ori_page_id;
        // Fields' offset inside Page's data
        PageFieldOffsetChecksums offsets;

        /// The meta and data may not be the same PageFile, (read_buffer == nullptr)
        /// use `target_file_id`, `page_offset`, `page_checksum` to indicate where
        /// data is actually store in.
        /// Should only use by `UPSERT` now.

        UInt64 page_offset;
        UInt64 page_checksum;
        PageFileIdAndLevel target_file_id;
    };
    using Writes = std::vector<Write>;

public:
    UniversalWriteBatch() = default;

    UniversalWriteBatch(UniversalWriteBatch && rhs)
        : writes(std::move(rhs.writes))
        , total_data_size(rhs.total_data_size)
    {}

    static UniversalWriteBatch fromWriteBatch(const String & prefix, WriteBatch && batch)
    {
        UniversalWriteBatch us_batch;
        const auto & writes = batch.getWrites();
        auto & us_writes = us_batch.getWrites();
        auto ns_id = batch.getNamespaceId();
        for (const auto & w : writes)
        {
            us_writes.push_back(Write{
                .type = w.type,
                .page_id = buildTableUniversalPageId(prefix, ns_id, w.page_id),
                .tag = w.tag,
                .read_buffer = w.read_buffer,
                .size = w.size,
                .ori_page_id = buildTableUniversalPageId(prefix, ns_id, w.ori_page_id),
                .offsets = std::move(w.offsets),
                .page_offset = w.page_offset,
                .page_checksum = w.page_checksum,
                .target_file_id = w.target_file_id
            });
        }
        us_batch.total_data_size = batch.getTotalDataSize();
        return us_batch;
    }

    void putPage(UniversalPageId page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size, const PageFieldSizes & data_sizes = {})
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

        Write w{WriteBatchWriteType::PUT, page_id, tag, read_buffer, size, "", std::move(offsets), 0, 0, {}};
        total_data_size += size;
        writes.emplace_back(std::move(w));
    }
    
    void putPage(UniversalPageId page_id, UInt64 tag, std::string_view data)
    {
        auto buffer_ptr = std::make_shared<ReadBufferFromOwnString>(data);
        putPage(page_id, tag, buffer_ptr, data.size());
    }

    void putExternal(UniversalPageId page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w{WriteBatchWriteType::PUT_EXTERNAL, page_id, tag, nullptr, 0, "", {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(UniversalPageId ref_id, UniversalPageId page_id)
    {
        Write w{WriteBatchWriteType::REF, ref_id, 0, nullptr, 0, page_id, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void delPage(UniversalPageId page_id)
    {
        Write w{WriteBatchWriteType::DEL, page_id, 0, nullptr, 0, "", {}, 0, 0, {}};
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

    void swap(UniversalWriteBatch & o)
    {
        writes.swap(o.writes);
        std::swap(o.total_data_size, total_data_size);
    }

    void merge(UniversalWriteBatch & rhs)
    {
        writes.reserve(writes.size() + rhs.writes.size());
        for (const auto & r : rhs.writes)
        {
            writes.emplace_back(r);
        }
    }

    void clear()
    {
        Writes tmp;
        writes.swap(tmp);
        total_data_size = 0;
    }

    size_t getTotalDataSize() const
    {
        return total_data_size;
    }

    static const UniversalPageId & getFullPageId(const UniversalPageId & id) { return id; }

    String toString() const
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.joinStr(
            writes.begin(),
            writes.end(),
            [](const auto & w, FmtBuffer & fb) {
                switch (w.type)
                {
                case WriteBatchWriteType::PUT:
                    fb.fmtAppend("{}", w.page_id);
                    break;
                case WriteBatchWriteType::REF:
                    fb.fmtAppend("{} > {}", w.page_id, w.ori_page_id);
                    break;
                case WriteBatchWriteType::DEL:
                    fb.fmtAppend("X{}", w.page_id);
                    break;
                case WriteBatchWriteType::PUT_EXTERNAL:
                    fb.fmtAppend("E{}", w.page_id);
                    break;
                default:
                    fb.fmtAppend("Unknow {}", w.page_id);
                    break;
                };
            },
            ",");
        return fmt_buffer.toString();
    }

private:
    Writes writes;
    size_t total_data_size = 0;
};
} // namespace DB

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

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class UniversalWriteBatch : private boost::noncopyable
{
public:
    enum class WriteType : UInt8
    {
        DEL = 0,
        // Create / Update a page, will implicitly create a RefPage{id} -> Page{id}.
        PUT = 1,
        // Create a RefPage{ref_id} -> Page{id}
        REF = 2,
        // Create an external page.
        // In V2, it is the same as `PUT`; In V3, we treated it as a different type from `PUT`
        // to get its lifetime management correct.
        PUT_EXTERNAL = 4,
    };

    using SequenceID = UInt64;

private:
    struct Write
    {
        WriteType type;
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

        Write w{WriteType::PUT, page_id, tag, read_buffer, size, "", std::move(offsets), 0, 0, {}};
        total_data_size += size;
        writes.emplace_back(std::move(w));
    }

    void putExternal(UniversalPageId page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w{WriteType::PUT_EXTERNAL, page_id, tag, nullptr, 0, "", {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(UniversalPageId ref_id, UniversalPageId page_id)
    {
        Write w{WriteType::REF, ref_id, 0, nullptr, 0, page_id, {}, 0, 0, {}};
        writes.emplace_back(std::move(w));
    }

    void delPage(UniversalPageId page_id)
    {
        Write w{WriteType::DEL, page_id, 0, nullptr, 0, "", {}, 0, 0, {}};
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
            count += (w.type == WriteType::PUT);
        return count;
    }

    void swap(UniversalWriteBatch & o)
    {
        writes.swap(o.writes);
        std::swap(o.total_data_size, total_data_size);
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

    String toString() const
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.joinStr(
            writes.begin(),
            writes.end(),
            [](const auto & w, FmtBuffer & fb) {
                switch (w.type)
                {
                case WriteType::PUT:
                    fb.fmtAppend("{}", w.page_id);
                    break;
                case WriteType::REF:
                    fb.fmtAppend("{} > {}", w.page_id, w.ori_page_id);
                    break;
                case WriteType::DEL:
                    fb.fmtAppend("X{}", w.page_id);
                    break;
                case WriteType::PUT_EXTERNAL:
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

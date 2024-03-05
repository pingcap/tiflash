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

#include <IO/Buffer/ReadBufferFromString.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/WriteBatchImpl.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

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

        std::optional<PS::V3::CheckpointLocation> data_location = std::nullopt;
    };
    using Writes = std::vector<Write>;

public:
    explicit UniversalWriteBatch(String prefix_ = "")
        : prefix(std::move(prefix_))
    {}

    void putPage(
        PageIdU64 page_id,
        UInt64 tag,
        const ReadBufferPtr & read_buffer,
        PageSize size,
        const PageFieldSizes & data_sizes = {})
    {
        putPage(UniversalPageIdFormat::toFullPageId(prefix, page_id), tag, read_buffer, size, data_sizes);
    }

    void putRemotePage(
        PageIdU64 page_id,
        UInt64 tag,
        PageSize size,
        const PS::V3::CheckpointLocation & data_location,
        PageFieldOffsetChecksums && offset_and_checksums)
    {
        putRemotePage(
            UniversalPageIdFormat::toFullPageId(prefix, page_id),
            tag,
            size,
            data_location,
            std::move(offset_and_checksums));
    }

    void putExternal(PageIdU64 page_id, UInt64 tag)
    {
        putExternal(UniversalPageIdFormat::toFullPageId(prefix, page_id), tag);
    }

    void putRemoteExternal(PageIdU64 page_id, const PS::V3::CheckpointLocation & data_location)
    {
        putRemoteExternal(UniversalPageIdFormat::toFullPageId(prefix, page_id), data_location);
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageIdU64 ref_id, PageIdU64 page_id)
    {
        putRefPage(
            UniversalPageIdFormat::toFullPageId(prefix, ref_id),
            UniversalPageIdFormat::toFullPageId(prefix, page_id));
    }

    void delPage(PageIdU64 page_id) { delPage(UniversalPageIdFormat::toFullPageId(prefix, page_id)); }

    void putPage(
        const UniversalPageId & page_id,
        UInt64 tag,
        const ReadBufferPtr & read_buffer,
        PageSize size,
        const PageFieldSizes & data_sizes = {})
    {
        // Convert from data_sizes to the offset of each field
        PageFieldOffsetChecksums offsets;
        PageFieldOffset off = 0;
        for (auto data_sz : data_sizes)
        {
            offsets.emplace_back(off, 0);
            off += data_sz;
        }

        RUNTIME_CHECK_MSG(
            data_sizes.empty() || off == size,
            "Try to put Page with fields, but page size and fields total size not match "
            "[page_id={}] [num_fields={}] [page_size={}] [all_fields_size={}]",
            page_id,
            data_sizes.size(),
            size,
            off);

        Write w{WriteBatchWriteType::PUT, page_id, tag, read_buffer, size, "", std::move(offsets)};
        total_data_size += size;
        writes.emplace_back(std::move(w));
    }

    void putPage(
        const UniversalPageId & page_id,
        UInt64 tag,
        std::string_view data,
        const PageFieldSizes & data_sizes = {})
    {
        auto buffer_ptr = std::make_shared<ReadBufferFromOwnString>(data);
        putPage(page_id, tag, buffer_ptr, data.size(), data_sizes);
    }

    void putRemotePage(
        const UniversalPageId & page_id,
        UInt64 tag,
        PageSize size,
        const PS::V3::CheckpointLocation & data_location,
        PageFieldOffsetChecksums && offset_and_checksums)
    {
        Write w{
            WriteBatchWriteType::PUT_REMOTE,
            page_id,
            tag,
            nullptr,
            size,
            "",
            std::move(offset_and_checksums),
            data_location};
        writes.emplace_back(std::move(w));
        has_writes_from_remote = true;
    }

    void updateRemotePage(const UniversalPageId & page_id, const ReadBufferPtr & read_buffer, PageSize size)
    {
        Write w{WriteBatchWriteType::UPDATE_DATA_FROM_REMOTE, page_id, 0, read_buffer, size, "", {}};
        total_data_size += size;
        writes.emplace_back(std::move(w));
        // This is use for update local page data from remote, don't need to set `has_writes_from_remote`
    }

    void putExternal(const UniversalPageId & page_id, UInt64 tag)
    {
        // External page's data is not managed by PageStorage, which means data is empty.
        Write w{WriteBatchWriteType::PUT_EXTERNAL, page_id, tag, nullptr, 0, "", {}};
        writes.emplace_back(std::move(w));
    }

    void putRemoteExternal(const UniversalPageId & page_id, const PS::V3::CheckpointLocation & data_location)
    {
        // TODO: do we need another write type for PUT_REMOTE_EXTERNAL?
        Write w{WriteBatchWriteType::PUT_EXTERNAL, page_id, /*tag*/ 0, nullptr, 0, "", {}, data_location};
        writes.emplace_back(std::move(w));
        has_writes_from_remote = true;
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(const UniversalPageId & ref_id, const UniversalPageId & page_id)
    {
        Write w{WriteBatchWriteType::REF, ref_id, 0, nullptr, 0, page_id, {}};
        writes.emplace_back(std::move(w));
    }

    void delPage(const UniversalPageId & page_id)
    {
        Write w{WriteBatchWriteType::DEL, page_id, 0, nullptr, 0, "", {}};
        writes.emplace_back(std::move(w));
    }

    bool empty() const { return writes.empty(); }

    size_t size() const { return writes.size(); }

    const Writes & getWrites() const { return writes; }
    Writes & getMutWrites() { return writes; }

    size_t putWriteCount() const
    {
        size_t count = 0;
        for (const auto & w : writes)
            count += (w.type == WriteBatchWriteType::PUT);
        return count;
    }

    // This write batch contains any `putRemotePage` or `putRemoteExternal`
    bool hasWritesFromRemote() const { return !remote_lock_disabled && has_writes_from_remote; }

    // There are some cases that we don't want to do remote lock when write to ps:
    // 1. Parse checkpoint files and write its contents to a temp ps for later use when do FAP;
    // 2. When do some tests which just involves read/write logic;
    void disableRemoteLock() { remote_lock_disabled = true; }

    size_t getTotalDataSize() const { return total_data_size; }

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
                case WriteBatchWriteType::PUT_REMOTE:
                    fb.fmtAppend("R{}", w.page_id);
                    break;
                default:
                    fb.fmtAppend("Unknown {}", w.page_id);
                    break;
                };
            },
            ",");
        return fmt_buffer.toString();
    }

    void merge(UniversalWriteBatch & rhs)
    {
        writes.reserve(writes.size() + rhs.writes.size());
        for (const auto & r : rhs.writes)
        {
            writes.emplace_back(r);
        }
        total_data_size += rhs.total_data_size;
        has_writes_from_remote |= rhs.has_writes_from_remote;
    }

    [[clang::reinitializes]] void clear()
    {
        Writes tmp;
        writes.swap(tmp);
        total_data_size = 0;
        has_writes_from_remote = false;
        remote_lock_disabled = false;
    }

    UniversalWriteBatch(UniversalWriteBatch && rhs) noexcept
        : prefix(std::move(rhs.prefix))
        , writes(std::move(rhs.writes))
        , total_data_size(rhs.total_data_size)
        , has_writes_from_remote(rhs.has_writes_from_remote)
        , remote_lock_disabled(rhs.remote_lock_disabled)
    {}

    void swap(UniversalWriteBatch & o)
    {
        prefix.swap(o.prefix);
        writes.swap(o.writes);
        std::swap(total_data_size, o.total_data_size);
        std::swap(has_writes_from_remote, o.has_writes_from_remote);
        std::swap(remote_lock_disabled, o.remote_lock_disabled);
    }

private:
    String prefix;
    Writes writes;
    size_t total_data_size = 0;
    bool has_writes_from_remote = false;
    bool remote_lock_disabled = false;
};
} // namespace DB

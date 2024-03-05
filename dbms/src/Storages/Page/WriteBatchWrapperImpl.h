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

#include <IO/Buffer/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <fmt/format.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

// It contains either an UniversalWriteBatch or a WriteBatch.
class WriteBatchWrapper : private boost::noncopyable
{
public:
    explicit WriteBatchWrapper(PageStorageRunMode mode, KeyspaceID keyspace_id, StorageType tag, NamespaceID ns_id)
    {
        switch (mode)
        {
        case PageStorageRunMode::UNI_PS:
            uwb = std::make_unique<UniversalWriteBatch>(UniversalPageIdFormat::toFullPrefix(keyspace_id, tag, ns_id));
            wb = nullptr;
            break;
        default:
            wb = std::make_unique<WriteBatch>(ns_id);
            uwb = nullptr;
            break;
        }
    }

    explicit WriteBatchWrapper(PageStorageRunMode mode, std::variant<String, NamespaceID> && prefix)
    {
        switch (mode)
        {
        case PageStorageRunMode::UNI_PS:
            uwb = std::make_unique<UniversalWriteBatch>(std::move(std::get<String>(prefix)));
            wb = nullptr;
            break;
        default:
            wb = std::make_unique<WriteBatch>(std::get<NamespaceID>(prefix));
            uwb = nullptr;
            break;
        }
    }

#ifdef DBMS_PUBLIC_GTEST
    WriteBatchWrapper(WriteBatch && wb_) // NOLINT(google-explicit-constructor), for gtest
        : wb(std::make_unique<WriteBatch>(std::move(wb_)))
        , uwb(nullptr)
    {}
#endif

    WriteBatchWrapper(WriteBatchWrapper && rhs)
        : wb(std::move(rhs.wb))
        , uwb(std::move(rhs.uwb))
    {}

    void putPage(
        PageIdU64 page_id,
        UInt64 tag,
        const ReadBufferPtr & read_buffer,
        PageSize size,
        const PageFieldSizes & data_sizes = {})
    {
        if (wb)
            wb->putPage(page_id, tag, read_buffer, size, data_sizes);
        else
            uwb->putPage(page_id, tag, read_buffer, size, data_sizes);
    }

    void putPage(PageIdU64 page_id, UInt64 tag, std::string_view data)
    {
        auto buffer_ptr = std::make_shared<ReadBufferFromOwnString>(data);
        putPage(page_id, tag, buffer_ptr, data.size());
    }

    void putRemotePage(
        PageIdU64 page_id,
        UInt64 tag,
        PageSize size,
        const PS::V3::CheckpointLocation & data_location,
        PageFieldOffsetChecksums && offset_and_checksums)
    {
        if (uwb)
            uwb->putRemotePage(page_id, tag, size, data_location, std::move(offset_and_checksums));
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "try to put remote page with remote location with u64 id, page_id={}",
                page_id);
    }

    void putExternal(PageIdU64 page_id, UInt64 tag)
    {
        if (wb)
            wb->putExternal(page_id, tag);
        else
            uwb->putExternal(page_id, tag);
    }

    void putRemoteExternal(PageIdU64 page_id, const PS::V3::CheckpointLocation & data_location)
    {
        if (uwb)
            uwb->putRemoteExternal(page_id, data_location);
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "try to put external page with remote location with u64 id, page_id={}",
                page_id);
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageIdU64 ref_id, PageIdU64 page_id)
    {
        if (wb)
            wb->putRefPage(ref_id, page_id);
        else
            uwb->putRefPage(ref_id, page_id);
    }

    void delPage(PageIdU64 page_id)
    {
        if (wb)
            wb->delPage(page_id);
        else
            uwb->delPage(page_id);
    }

    bool empty() const
    {
        if (wb)
            return wb->empty();
        else
            return uwb->empty();
    }

    size_t size() const
    {
        if (wb)
            return wb->size();
        else
            return uwb->size();
    }

    void clear()
    {
        if (wb)
            wb->clear();
        else
            uwb->clear();
    }

    const WriteBatch & getWriteBatch() const { return *wb; }

    const UniversalWriteBatch & getUniversalWriteBatch() const { return *uwb; }

    WriteBatch && releaseWriteBatch() { return std::move(*wb); }

    UniversalWriteBatch && releaseUniversalWriteBatch() { return std::move(*uwb); }

private:
    std::unique_ptr<WriteBatch> wb;
    std::unique_ptr<UniversalWriteBatch> uwb;
};
} // namespace DB

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
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchAdaptor.h>
#include <Storages/Page/WriteBatch.h>
#include <fmt/format.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

enum class PageStorageRunMode : UInt8
{
    ONLY_V2 = 1,
    ONLY_V3 = 2,
    MIX_MODE = 3,
    UNI_PS = 4,
};

enum class StorageType
{
    Log = 1,
    Data = 2,
    Meta = 3,
    KVStore = 4,
};

static inline String combine(const String & prefix, NamespaceId ns_id)
{
    WriteBufferFromOwnString buff;
    writeString(prefix, buff);
    UniversalPageIdFormat::encodeUInt64(ns_id, buff);
    return buff.releaseStr();
}

static inline String getTableStoragePrefix(StorageType tag, NamespaceId ns_id)
{
    switch (tag)
    {
    // TODO: remove hardcode prefix here
    case StorageType::Log:
        return combine("tl", ns_id);
    case StorageType::Data:
        return combine("td", ns_id);
    case StorageType::Meta:
        return combine("tm", ns_id);
    case StorageType::KVStore:
        return "kvs";
    default:
        throw Exception(fmt::format("Unknown storage tag {}", static_cast<UInt8>(tag)), ErrorCodes::LOGICAL_ERROR);
    }
}

class WriteBatchWrapper : private boost::noncopyable
{
public:
    explicit WriteBatchWrapper(PageStorageRunMode mode, StorageType tag, NamespaceId ns_id)
    {
        switch (mode)
        {
        case PageStorageRunMode::UNI_PS:
            uwb = std::make_unique<UniversalWriteBatchAdaptor>(getTableStoragePrefix(tag, ns_id));
            wb = nullptr;
            break;
        default:
            wb = std::make_unique<WriteBatch>(ns_id);
            uwb = nullptr;
            break;
        }
    }

    explicit WriteBatchWrapper(PageStorageRunMode mode, std::variant<String, NamespaceId> && prefix)
    {
        switch (mode)
        {
        case PageStorageRunMode::UNI_PS:
            uwb = std::make_unique<UniversalWriteBatchAdaptor>(std::move(std::get<String>(prefix)));
            wb = nullptr;
            break;
        default:
            wb = std::make_unique<WriteBatch>(std::get<NamespaceId>(prefix));
            uwb = nullptr;
            break;
        }
    }

    WriteBatchWrapper(WriteBatchWrapper && rhs)
        : wb(std::move(rhs.wb))
        , uwb(std::move(rhs.uwb))
    {}

    void putPage(PageIdU64 page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size, const PageFieldSizes & data_sizes = {})
    {
        if (wb)
            wb->putPage(page_id, tag, read_buffer, size, data_sizes);
        else
            uwb->putPage(page_id, tag, read_buffer, size, data_sizes);
    }

    void putExternal(PageIdU64 page_id, UInt64 tag)
    {
        if (wb)
            wb->putExternal(page_id, tag);
        else
            uwb->putExternal(page_id, tag);
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

    void clear()
    {
        if (wb)
            wb->clear();
        else
            uwb->clear();
    }

    const WriteBatch & getWriteBatch() const
    {
        return *wb;
    }

    const UniversalWriteBatch & getUniversalWriteBatch() const
    {
        return uwb->getUniversalWriteBatch();
    }

    WriteBatch && releaseWriteBatch()
    {
        return std::move(*wb);
    }

    UniversalWriteBatch && releaseUniversalWriteBatch()
    {
        return uwb->releaseUniversalWriteBatch();
    }

private:
    std::unique_ptr<WriteBatch> wb;
    std::unique_ptr<UniversalWriteBatchAdaptor> uwb;
};
} // namespace DB

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

#include <Storages/Page/universal/UniversalPageStorage.h>

namespace DB
{


class KVStoreReader final
{
public:
    explicit KVStoreReader(UniversalPageStorage & storage)
        : uni_storage(storage)
    {}

    static UniversalPageId toFullPageId(PageId page_id)
    {
        // TODO: Does it need to be mem comparable?
        WriteBufferFromOwnString buff;
        writeString("k_", buff);
        UniversalPageIdFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
    }

    static PageId parseRegionId(const UniversalPageId & u_id)
    {
        return UniversalPageIdFormat::decodeUInt64(u_id.data() + u_id.size() - sizeof(PageId));
    }

    bool isAppliedIndexExceed(PageId page_id, UInt64 applied_index)
    {
        // assume use this format
        UniversalPageId uni_page_id = toFullPageId(page_id);
        auto snap = uni_storage.getSnapshot("");
        const auto & [id, entry] = uni_storage.page_directory->getByIDOrNull(uni_page_id, snap);
        return (entry.isValid() && entry.tag > applied_index);
    }

    void read(const PageIds & /*page_ids*/, std::function<void(const PageId &, const Page &)> /*handler*/) const
    {
        // UniversalPageIds uni_page_ids;
        // uni_page_ids.reserve(page_ids.size());
        // for (const auto & pid : page_ids)
        //     uni_page_ids.emplace_back(toFullPageId(pid));
        // auto snap = uni_storage.getSnapshot("");
        // auto page_entries = uni_storage.page_directory->getByIDs(uni_page_ids, snap);
        // uni_storage.blob_store->read(page_entries, handler, nullptr);
        throw Exception("Not implemented");
    }

    void traverse(const std::function<void(DB::PageId page_id, const DB::Page & page)> & acceptor)
    {
        // Only traverse pages with id prefix
        auto snapshot = uni_storage.getSnapshot("scan_region_persister");
        // FIXME: use a better end
        const auto page_ids = uni_storage.page_directory->getRangePageIds("k_", "l_");
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(parseRegionId(page_id), uni_storage.blob_store->read(page_id_and_entry));
        }
    }

private:
    UniversalPageStorage & uni_storage;
};
using KVStoreReaderPtr = std::shared_ptr<KVStoreReader>;

class RaftLogReader final
{
public:
    explicit RaftLogReader(UniversalPageStorage & storage)
        : uni_storage(storage)
    {}

    static UniversalPageId toFullPageId(UInt64 ns_id, PageId page_id)
    {
        WriteBufferFromOwnString buff;
        writeString("r_", buff);
        UniversalPageIdFormat::encodeUInt64(ns_id, buff);
        writeString("_", buff);
        UniversalPageIdFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
        // return fmt::format("r_{}_{}", ns_id, page_id);
    }

    // 0x01 0x02 region_id 0x01
    static UniversalPageId toFullRaftLogPrefix(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        // BigEndian
        UniversalPageIdFormat::encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    // 0x01 0x02 region_id 0x01 log_idx
    static UniversalPageId toFullRaftLogKey(UInt64 region_id, PageId log_index)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        // BigEndian
        UniversalPageIdFormat::encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        UniversalPageIdFormat::encodeUInt64(log_index, buff);
        return buff.releaseStr();
    }

    // 0x01 0x03 region_id
    static UniversalPageId toRegionMetaPrefixKey(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x03, buff);
        // BigEndian
        UniversalPageIdFormat::encodeUInt64(region_id, buff);
        return buff.releaseStr();
    }

    // 0x01 0x03 region_id 0x01
    static UniversalPageId toRegionLocalStateKey(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x03, buff);
        // BigEndian
        UniversalPageIdFormat::encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    // 0x01 0x02 region_id 0x03
    static UniversalPageId toRegionApplyStateKey(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x03, buff);
        // BigEndian
        UniversalPageIdFormat::encodeUInt64(region_id, buff);
        writeChar(0x03, buff);
        return buff.releaseStr();
    }

    // scan the pages between [start, end)
    void traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const UniversalPageId & page_id, const DB::Page & page)> & acceptor)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
        const auto page_ids = uni_storage.page_directory->getRangePageIds(start, end);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(page_id, uni_storage.blob_store->read(page_id_and_entry));
        }
    }

    Page read(const UniversalPageId & page_id)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("read_{}", page_id));
        const auto page_id_and_entry = uni_storage.page_directory->getByIDOrNull(page_id, snapshot);
        if (page_id_and_entry.second.isValid())
        {
            return uni_storage.blob_store->read(page_id_and_entry);
        }
        else
        {
            return DB::Page::invalidPage();
        }
    }

    UniversalPageIds getLowerBound(const UniversalPageId & page_id)
    {
        return uni_storage.page_directory->getLowerBound(page_id);
    }

private:
    UniversalPageStorage & uni_storage;
};

class StorageReader final
{
public:
    explicit StorageReader(UniversalPageStorage & storage, const String & prefix_, NamespaceId ns_id_)
        : uni_storage(storage)
        , prefix(prefix_)
        , ns_id(ns_id_)
    {
    }

    UniversalPageId toPrefix() const
    {
        WriteBufferFromOwnString buff;
        writeString(prefix, buff);
        UniversalPageIdFormat::encodeUInt64(ns_id, buff);
        writeString("_", buff);
        return buff.releaseStr();
    }

    static UniversalPageId toFullUniversalPageId(const String & prefix, NamespaceId ns_id, PageId page_id)
    {
        return buildTableUniversalPageId(prefix, ns_id, page_id);
    }

    UniversalPageId toFullPageId(PageId page_id) const
    {
        return buildTableUniversalPageId(prefix, ns_id, page_id);
    }

    UniversalPageIds toFullPageIds(PageIds page_ids) const
    {
        UniversalPageIds us_page_ids;
        for (const auto page_id : page_ids)
        {
            us_page_ids.push_back(toFullPageId(page_id));
        }
        return us_page_ids;
    }

    static PageId parsePageId(const UniversalPageId & u_id)
    {
        return DB::PS::V3::universal::ExternalIdTrait::getU64ID(u_id);
    }

    void traverse(const std::function<void(DB::PageId page_id, const DB::Page & page)> & acceptor) const
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_{}_{}", prefix, ns_id));
        const auto page_ids = uni_storage.page_directory->getPageIdsWithPrefix(toPrefix());
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(parsePageId(page_id_and_entry.first), uni_storage.blob_store->read(page_id_and_entry));
        }
    }

private:
    UniversalPageStorage & uni_storage;
    String prefix;
    NamespaceId ns_id;
};

} // namespace DB

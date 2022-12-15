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
        return fmt::format("k_{}", page_id);
    }

    bool isAppliedIndexExceed(PageId page_id, UInt64 applied_index)
    {
        // assume use this format
        UniversalPageId uni_page_id = toFullPageId(page_id);
        auto snap = uni_storage.getSnapshot("");
        const auto & [id, entry] = uni_storage.page_directory->getByIDOrNull(uni_page_id, snap);
        return (entry.isValid() && entry.tag > applied_index);
    }

    void read(const PageIds & /*page_ids*/, std::function<void(const PageId &, const UniversalPage &)> /*handler*/) const
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

    void traverse(const std::function<void(const DB::UniversalPage & page)> & /*acceptor*/)
    {
        // Only traverse pages with id prefix
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    UniversalPageStorage & uni_storage;
};

class RaftLogReader final
{
public:
    explicit RaftLogReader(UniversalPageStorage & storage)
        : uni_storage(storage)
    {}

    static UniversalPageId toFullPageId(NamespaceId ns_id, PageId page_id)
    {
        // TODO: Does it need to be mem comparable?
        WriteBufferFromOwnString buff;
        writeString("r_", buff);
        RecordKVFormat::encodeUInt64(ns_id, buff);
        writeString("_", buff);
        RecordKVFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
        // return fmt::format("r_{}_{}", ns_id, page_id);
    }

    // scan the pages between [start, end)
    void traverse(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(const DB::UniversalPage & page)> & acceptor)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
        const auto page_ids = uni_storage.page_directory->getRangePageIds(start, end);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(uni_storage.blob_store->read(page_id_and_entry));
        }
    }

    void traverse2(const UniversalPageId & start, const UniversalPageId & end, const std::function<void(DB::UniversalPage page)> & acceptor)
    {
        // always traverse with the latest snapshot
        auto snapshot = uni_storage.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
        const auto page_ids = uni_storage.page_directory->getRangePageIds(start, end);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = uni_storage.page_directory->getByID(page_id, snapshot);
            acceptor(uni_storage.blob_store->read(page_id_and_entry));
        }
    }

    UniversalPage read(const UniversalPageId & page_id)
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
            return UniversalPage({});
        }
    }

    UniversalPageIds getLowerBound(const UniversalPageId & page_id)
    {
        return uni_storage.page_directory->getLowerBound(page_id);
    }

private:
    UniversalPageStorage & uni_storage;
};

class UniversalPageReader final
{
public:
    explicit UniversalPageReader(UniversalPageStoragePtr storage_, PageStorageSnapshotPtr snap_)
        : storage(storage_)
        , snap(snap_)
    {}

    UniversalPage read(const UniversalPageId & page_id) const
    {
        // TODO: Move primitive read functions into UniversalPageStorage.
        const auto page_id_and_entry = storage->page_directory->getByIDOrNull(page_id, snap);
        if (page_id_and_entry.second.isValid())
        {
            return storage->blob_store->read(page_id_and_entry);
        }
        else
        {
            return UniversalPage({});
        }
    }

    using PageReadFields = UniversalPageStorage::PageReadFields;
    UniversalPageMap read(const std::vector<PageReadFields> & page_fields) const
    {
        return storage->read(page_fields, nullptr, snap);
    }

private:
    UniversalPageStoragePtr storage;
    PageStorageSnapshotPtr snap;
};

using UniversalPageReaderPtr = std::shared_ptr<UniversalPageReader>;

} // namespace DB

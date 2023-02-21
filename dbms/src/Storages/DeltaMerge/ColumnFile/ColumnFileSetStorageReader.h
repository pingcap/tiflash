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
#include <Storages/DeltaMerge/Remote/Manager.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageDefines.h>

namespace DB::DM
{

class IColumnFileSetStorageReader
{
public:
    virtual ~IColumnFileSetStorageReader() = default;

    /// TODO: Unify these interfaces to be a reader mode.

    /// Read some fields from the page. Used when reading from Column File.
    virtual Page readForColumnFileTiny(const PageStorage::PageReadFields &) const = 0;

    virtual size_t getColumnFileTinySerializedSize(PageId) const = 0;

    /// Read the whole page. Used when Write Node pass data to Read Node.
    /// TODO: Remove this. Actually ColumnFile does not need this interface...
    virtual Page readForColumnFileTiny(PageId) const = 0;
};

using IColumnFileSetStorageReaderPtr = std::shared_ptr<IColumnFileSetStorageReader>;

class LocalColumnFileSetStorage : public IColumnFileSetStorageReader
{
private:
    /// Although we only use its log_reader member, we still want to keep the whole
    /// storage snapshot valid, because some Column Files like Column File Big relies
    /// on specific DMFile to be valid, whose lifecycle is maintained by other reader members.
    StorageSnapshotPtr storage_snap;

public:
    explicit LocalColumnFileSetStorage(StorageSnapshotPtr storage_snap_)
        : storage_snap(storage_snap_)
    {}

    Page readForColumnFileTiny(
        const PageStorage::PageReadFields & fields) const override
    {
        auto page_map = storage_snap->log_reader.read({fields});
        return page_map[fields.first];
    }

    size_t getColumnFileTinySerializedSize(PageId page_id) const override
    {
        return storage_snap->log_reader.getPageEntry(page_id).size;
    }

    Page readForColumnFileTiny(PageId page_id) const override
    {
        auto page = storage_snap->log_reader.read(page_id);
        return page;
    }
};

class DummyColumnFileSetStorage : public IColumnFileSetStorageReader
{
public:
    Page readForColumnFileTiny(
        const PageStorage::PageReadFields &) const override
    {
        RUNTIME_CHECK_MSG(false, "DummyColumnFileSetStorage cannot read");
    }

    size_t getColumnFileTinySerializedSize(PageId) const override
    {
        RUNTIME_CHECK_MSG(false, "DummyColumnFileSetStorage cannot read");
    }

    Page readForColumnFileTiny(PageId) const override
    {
        RUNTIME_CHECK_MSG(false, "DummyColumnFileSetStorage cannot read");
    }
};

class RemoteColumnFileSetStorage : public IColumnFileSetStorageReader
{
private:
    Remote::LocalPageCachePtr page_cache;
    Remote::LocalPageCacheGuardPtr pages_guard; // Only keep for maintaining lifetime for related keys

    UInt64 write_node_id;
    Int64 table_id;

public:
    explicit RemoteColumnFileSetStorage(
        Remote::LocalPageCachePtr page_cache_,
        Remote::LocalPageCacheGuardPtr guard,
        UInt64 write_node_id_,
        Int64 table_id_)
        : page_cache(page_cache_)
        , pages_guard(guard)
        , write_node_id(write_node_id_)
        , table_id(table_id_)
    {}

    Page readForColumnFileTiny(
        const PageStorage::PageReadFields & fields) const override;

    size_t getColumnFileTinySerializedSize(PageId) const override
    {
        RUNTIME_CHECK(false);
    }

    Page readForColumnFileTiny(PageId) const override
    {
        RUNTIME_CHECK(false);
    }
};

} // namespace DB::DM

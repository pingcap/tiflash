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

#include <Storages/DeltaMerge/Remote/Manager.h>
#include <Storages/Page/PageDefines.h>

namespace DB::DM
{

namespace Remote
{
class Manager;
using ManagerPtr = std::shared_ptr<Manager>;
} // namespace Remote

class IColumnFileSetStorageReader
{
public:
    virtual ~IColumnFileSetStorageReader() = default;

    virtual Page readForColumnFileTiny(const PageStorage::PageReadFields &) const = 0;
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
};

class RemoteColumnFileSetStorage : public IColumnFileSetStorageReader
{
private:
    Remote::LocalPageCachePtr page_cache;
    // TODO: Keep a snapshot of page_cache here.

    UInt64 write_node_id;
    Int64 table_id;

public:
    explicit RemoteColumnFileSetStorage(
        Remote::ManagerPtr remote_manager,
        UInt64 write_node_id_,
        Int64 table_id_)
        : page_cache(remote_manager->getPageCache())
        , write_node_id(write_node_id_)
        , table_id(table_id_)
    {}

    Page readForColumnFileTiny(
        const PageStorage::PageReadFields & fields) const override
    {
        auto oid = Remote::PageOID{
            .write_node_id = write_node_id,
            .table_id = table_id,
            .page_id = fields.first,
        };
        return page_cache->getPage(oid, fields.second);
    }
};

} // namespace DB::DM
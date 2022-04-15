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

#include <Common/Logger.h>
#include <Flash/Coprocessor/StorageWithStructureLock.h>
#include <Flash/Coprocessor/TiDBTableScan.h>

namespace DB
{
class Context;
class TMTContext;

class TiDBStorageTable
{
public:
    TiDBStorageTable(
        const tipb::Executor * table_scan_,
        const String & executor_id_,
        Context & context_,
        const String & req_id);

    const TiDBTableScan & getTiDBTableScan() const { return tidb_table_scan; }
    const NamesAndTypes & getSchema() const { return schema; }

    void releaseAlterLock();

    const TableLockHolders & getDropLocks() const;

    const ManageableStoragePtr & getStorage(TableID table_id) const;

    const ManageableStoragePtr & getLogicalTableStorage() const { return storage_for_logical_table; }

private:
    IDsAndStorageWithStructureLocks getAndLockStorages(const TiDBTableScan & table_scan);
    NamesAndTypes getSchemaForTableScan(const TiDBTableScan & table_scan);

private:
    Context & context;
    TMTContext & tmt;
    LoggerPtr log;

    TiDBTableScan tidb_table_scan;

    // after `releaseAlterLock`, all struct lock will call release and move to drop_locks.
    // and then alter_locks_released will change to true.
    bool alter_locks_released = false;
    /// Table from where to read data, if not subquery.
    /// Hold read lock on both `alter_lock` and `drop_lock` until the local input streams are created.
    /// We need an immutable structure to build the TableScan operator and create snapshot input streams
    /// of storage. After the input streams created, the `alter_lock` can be released so that reading
    /// won't block DDL operations.
    IDsAndStorageWithStructureLocks storages_with_structure_lock;
    TableLockHolders drop_locks;

    ManageableStoragePtr storage_for_logical_table;

    NamesAndTypes schema;
};
} // namespace DB
#pragma once

#include <Storages/IManageableStorage.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/Types.h>

#include <unordered_map>

namespace DB
{
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;
struct StorageWithStructureLock
{
    ManageableStoragePtr storage;
    TableStructureLockHolder lock;
};

using IDsAndStorageWithStructureLocks = std::unordered_map<TableID, StorageWithStructureLock>;
} // namespace DB
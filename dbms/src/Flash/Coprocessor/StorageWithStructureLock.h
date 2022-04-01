#pragma once

#include <Storages/TableLockHolder.h>
#include <Storages/IManageableStorage.h>

namespace DB
{
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;
struct StorageWithStructureLock
{
    ManageableStoragePtr storage;
    TableStructureLockHolder lock;
};
}
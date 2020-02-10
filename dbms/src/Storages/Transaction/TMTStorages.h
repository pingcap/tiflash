#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include <Storages/Transaction/Types.h>

namespace DB
{

class IManageableStorage;
class StorageMergeTree;
class StorageDeltaMerge;
using StorageMergeTreePtr = std::shared_ptr<StorageMergeTree>;
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;

class ManagedStorages : private boost::noncopyable
{
public:
    void put(ManageableStoragePtr storage);

    ManageableStoragePtr get(TableID table_id) const;
    std::unordered_map<TableID, ManageableStoragePtr> getAllStorage() const;

    ManageableStoragePtr getByName(const std::string & db, const std::string & table) const;

    void remove(TableID table_id);

private:
    std::unordered_map<TableID, ManageableStoragePtr> storages;
    mutable std::mutex mutex;
};

} // namespace DB

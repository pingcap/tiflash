#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

void TMTStorages::put(TMTStoragePtr storage)
{
    std::lock_guard lock(mutex);

    TableID table_id = storage->getTableInfo().id;
    if (storages.find(table_id) != storages.end())
        return;
    storages.emplace(table_id, storage);
}

TMTStoragePtr TMTStorages::get(TableID table_id) const
{
    std::lock_guard lock(mutex);

    auto it = storages.find(table_id);
    if (it == storages.end())
        return nullptr;
    return it->second;
}

std::unordered_map<TableID, TMTStoragePtr> TMTStorages::getAllStorage() const
{
    std::lock_guard lock(mutex);
    return storages;
}

TMTStoragePtr TMTStorages::getByName(const std::string & db, const std::string & table) const
{
    std::lock_guard lock(mutex);

    auto it = std::find_if(storages.begin(), storages.end(), [&](const std::pair<TableID, StoragePtr> & pair) {
        auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(pair.second);
        return merge_tree->getDatabaseName() == db && merge_tree->getTableName() == table;
    });
    if (it == storages.end())
        return nullptr;
    return it->second;
}

void TMTStorages::remove(TableID table_id)
{
    std::lock_guard lock(mutex);

    auto it = storages.find(table_id);
    if (it == storages.end())
        return;
    storages.erase(it);
}

} // namespace DB

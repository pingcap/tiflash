#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TIDB_TABLE_ALREADY_EXISTS;
}

void ManagedStorages::put(ManageableStoragePtr storage)
{
    std::lock_guard lock(mutex);

    TableID table_id = storage->getTableInfo().id;
    if (storages.find(table_id) != storages.end() && table_id != DB::InvalidTableID)
    {
        // If table already exists, and is not created through ch-client (which table_id could be unspecified)
        // throw Exception
        throw Exception("TiDB table with id " + DB::toString(table_id) + " already exists.", ErrorCodes::TIDB_TABLE_ALREADY_EXISTS);
    }
    storages.emplace(table_id, storage);
}

ManageableStoragePtr ManagedStorages::get(TableID table_id) const
{
    std::lock_guard lock(mutex);

    if (auto it = storages.find(table_id); it != storages.end())
        return it->second;
    return nullptr;
}

std::unordered_map<TableID, ManageableStoragePtr> ManagedStorages::getAllStorage() const
{
    std::lock_guard lock(mutex);
    return storages;
}

ManageableStoragePtr ManagedStorages::getByName(const std::string & db, const std::string & table, bool include_tombstone) const
{
    std::lock_guard lock(mutex);

    auto it = std::find_if(storages.begin(), storages.end(), [&](const std::pair<TableID, ManageableStoragePtr> & pair) {
        const auto & storage = pair.second;
        return (include_tombstone || !storage->isTombstone()) && storage->getDatabaseName() == db && storage->getTableInfo().name == table;
    });
    if (it == storages.end())
        return nullptr;
    return it->second;
}

void ManagedStorages::remove(TableID table_id)
{
    std::lock_guard lock(mutex);

    auto it = storages.find(table_id);
    if (it == storages.end())
        return;
    storages.erase(it);
}

} // namespace DB

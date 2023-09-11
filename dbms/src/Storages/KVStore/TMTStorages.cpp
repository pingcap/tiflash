// Copyright 2023 PingCAP, Inc.
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

#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/TMTStorages.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TIDB_TABLE_ALREADY_EXISTS;
}

void ManagedStorages::put(ManageableStoragePtr storage)
{
    std::lock_guard lock(mutex);

    KeyspaceID keyspace_id = storage->getTableInfo().keyspace_id;
    TableID table_id = storage->getTableInfo().id;
    auto keyspace_table_id = KeyspaceTableID{keyspace_id, table_id};
    if (storages.find(keyspace_table_id) != storages.end() && table_id != DB::InvalidTableID)
    {
        // If table already exists, and is not created through ch-client (which table_id could be unspecified)
        // throw Exception
        throw Exception(
            "TiDB table with id " + DB::toString(table_id) + " already exists.",
            ErrorCodes::TIDB_TABLE_ALREADY_EXISTS);
    }
    storages.emplace(keyspace_table_id, storage);
    auto [it, _] = keyspaces.try_emplace(keyspace_id, 0);
    it->second++;
}

ManageableStoragePtr ManagedStorages::get(KeyspaceID keyspace_id, TableID table_id) const
{
    std::lock_guard lock(mutex);

    if (auto it = storages.find(KeyspaceTableID{keyspace_id, table_id}); it != storages.end())
        return it->second;
    return nullptr;
}

StorageMap ManagedStorages::getAllStorage() const
{
    std::lock_guard lock(mutex);
    return storages;
}

KeyspaceSet ManagedStorages::getAllKeyspaces() const
{
    std::lock_guard lock(mutex);
    return keyspaces;
}

ManageableStoragePtr ManagedStorages::getByName(
    const std::string & db,
    const std::string & table,
    bool include_tombstone) const
{
    std::lock_guard lock(mutex);

    auto it = std::find_if(
        storages.begin(),
        storages.end(),
        [&](const std::pair<KeyspaceTableID, ManageableStoragePtr> & pair) {
            const auto & storage = pair.second;
            return (include_tombstone || !storage->isTombstone()) && storage->getDatabaseName() == db
                && storage->getTableInfo().name == table;
        });
    if (it == storages.end())
        return nullptr;
    return it->second;
}

void ManagedStorages::remove(KeyspaceID keyspace_id, TableID table_id)
{
    std::lock_guard lock(mutex);

    auto it = storages.find(KeyspaceTableID{keyspace_id, table_id});
    if (it == storages.end())
        return;
    storages.erase(it);
    keyspaces[keyspace_id]--;
    if (!keyspaces[keyspace_id])
        keyspaces.erase(keyspace_id);
}

} // namespace DB

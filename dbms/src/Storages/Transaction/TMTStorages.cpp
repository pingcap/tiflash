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

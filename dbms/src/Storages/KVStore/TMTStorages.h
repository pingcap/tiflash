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

#pragma once

#include <Storages/KVStore/Types.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
class IManageableStorage;
class StorageDeltaMerge;
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;
using StorageMap = std::unordered_map<KeyspaceTableID, ManageableStoragePtr, boost::hash<KeyspaceTableID>>;
using KeyspaceSet = std::unordered_map<KeyspaceID, size_t>;

class ManagedStorages : private boost::noncopyable
{
public:
    void put(ManageableStoragePtr storage);

    // Get storage by keyspace and table id
    ManageableStoragePtr get(KeyspaceID keyspace_id, TableID table_id) const;
    // Get all the storages of all the keyspaces in this instance.
    StorageMap getAllStorage() const;
    // Get all the existing keyspaces in this instance. A map of `{KeySpaceID => num of physical tables}`.
    KeyspaceSet getAllKeyspaces() const;

    ManageableStoragePtr getByName(const std::string & db, const std::string & table, bool include_tombstone) const;

    void remove(KeyspaceID keyspace_id, TableID table_id);

private:
    StorageMap storages;
    KeyspaceSet keyspaces;
    // if we use shared_mutex, the performance under high concurrency is obviously worser than std::mutex
    mutable std::mutex mutex;
};

} // namespace DB

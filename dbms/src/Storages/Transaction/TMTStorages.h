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

#include <Storages/Transaction/Types.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
class IManageableStorage;
class StorageDeltaMerge;
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;

class ManagedStorages : private boost::noncopyable
{
public:
    void put(ManageableStoragePtr storage);

    ManageableStoragePtr get(TableID table_id) const;
    std::unordered_map<TableID, ManageableStoragePtr> getAllStorage() const;

    ManageableStoragePtr getByName(const std::string & db, const std::string & table, bool include_tombstone) const;

    void remove(TableID table_id);

private:
    std::unordered_map<TableID, ManageableStoragePtr> storages;
    mutable std::mutex mutex;
};

} // namespace DB

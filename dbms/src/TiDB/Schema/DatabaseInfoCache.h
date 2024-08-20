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
#include <TiDB/Schema/TiDB_fwd.h>

#include <shared_mutex>
#include <unordered_map>

namespace DB
{

class DatabaseInfoCache
{
public:
    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) const
    {
        std::shared_lock lock(mtx_databases);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) {
            return pair.second->name == database_name;
        });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    void addDatabaseInfo(const TiDB::DBInfoPtr & db_info)
    {
        std::unique_lock lock(mtx_databases);
        databases.emplace(db_info->id, db_info);
    }

    TiDB::DBInfoPtr getDBInfo(DatabaseID database_id) const
    {
        std::shared_lock shared_lock(mtx_databases);
        if (auto it = databases.find(database_id); likely(it != databases.end()))
        {
            return it->second;
        }
        return nullptr;
    }

    bool exists(DatabaseID database_id) const
    {
        std::shared_lock shared_lock(mtx_databases);
        return databases.contains(database_id);
    }

    void eraseDBInfo(DatabaseID database_id)
    {
        std::unique_lock shared_lock(mtx_databases);
        databases.erase(database_id);
    }

    void clear()
    {
        std::unique_lock lock(mtx_databases);
        databases.clear();
    }

private:
    mutable std::shared_mutex mtx_databases;
    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases;
};
} // namespace DB

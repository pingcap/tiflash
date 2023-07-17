// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Transaction/TiDB.h>

namespace DB
{

/// Always call functions in this class when trying to get the underlying entity (db/table/partition) name in CH.
struct SchemaNameMapper
{
    virtual ~SchemaNameMapper() = default;

    static constexpr auto DATABASE_PREFIX = "db_";
    static constexpr auto TABLE_PREFIX = "t_";
    static constexpr std::string_view KEYSPACE_PREFIX = "ks_";


    static KeyspaceID getMappedNameKeyspaceID(const String & name)
    {
        auto keyspace_prefix_len = KEYSPACE_PREFIX.length();
        auto pos = name.find(KEYSPACE_PREFIX);
        if (pos == String::npos)
            return NullspaceID;
        assert(pos == 0);
        pos = name.find('_', keyspace_prefix_len);
        assert(pos != String::npos);
        return std::stoull(name.substr(keyspace_prefix_len, pos - keyspace_prefix_len));
    }

    static String map2Keyspace(KeyspaceID keyspace_id, const String & name)
    {
        return keyspace_id == NullspaceID ? name : KEYSPACE_PREFIX.data() + std::to_string(keyspace_id) + "_" + name;
    }

    virtual String mapDatabaseName(DatabaseID db_id, KeyspaceID keyspace_id) const
    {
        auto db_name = DATABASE_PREFIX + std::to_string(db_id);
        return map2Keyspace(keyspace_id, db_name);
    }

    virtual String mapDatabaseName(const TiDB::DBInfo & db_info) const
    {
        auto db_name = DATABASE_PREFIX + std::to_string(db_info.id);
        return map2Keyspace(db_info.keyspace_id, db_name);
    }

    virtual String displayDatabaseName(const TiDB::DBInfo & db_info) const
    {
        return map2Keyspace(db_info.keyspace_id, db_info.name);
    }
    virtual String mapTableName(const TiDB::TableInfo & table_info) const
    {
        auto table_name = TABLE_PREFIX + std::to_string(table_info.id);
        return map2Keyspace(table_info.keyspace_id, table_name);
    }
    virtual String displayTableName(const TiDB::TableInfo & table_info) const
    {
        return map2Keyspace(table_info.keyspace_id, table_info.name);
    }
    virtual String mapPartitionName(const TiDB::TableInfo & table_info) const { return mapTableName(table_info); }

    // Only use for logging / debugging
    virtual String debugDatabaseName(const TiDB::DBInfo & db_info) const
    {
        auto db_name = db_info.name + "(" + std::to_string(db_info.id) + ")";
        return map2Keyspace(db_info.keyspace_id, db_name);
    }
    virtual String debugTableName(const TiDB::TableInfo & table_info) const
    {
        auto table_name = table_info.name + "(" + std::to_string(table_info.id) + ")";
        return map2Keyspace(table_info.keyspace_id, table_name);
    }
    virtual String debugCanonicalName(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info) const
    {
        return debugDatabaseName(db_info) + "." + debugTableName(table_info);
    }

    virtual String debugCanonicalName(const TiDB::TableInfo & table_info, DatabaseID db_id, KeyspaceID keyspace_id) const
    {
        auto db_name = DATABASE_PREFIX + std::to_string(db_id);
        return map2Keyspace(keyspace_id, db_name) + "." + debugTableName(table_info);
    }
};

} // namespace DB

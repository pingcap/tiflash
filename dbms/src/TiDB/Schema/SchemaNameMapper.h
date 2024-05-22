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

#include <Storages/Transaction/TiDB.h>

namespace DB
{

/// Always call functions in this class when trying to get the underlying entity (db/table/partition) name in CH.
struct SchemaNameMapper
{
    virtual ~SchemaNameMapper() = default;

    static constexpr auto DATABASE_PREFIX = "db_";
    static constexpr auto TABLE_PREFIX = "t_";

    virtual String mapDatabaseName(const TiDB::DBInfo & db_info) const { return DATABASE_PREFIX + std::to_string(db_info.id); }
    virtual String displayDatabaseName(const TiDB::DBInfo & db_info) const { return db_info.name; }
    virtual String mapTableName(const TiDB::TableInfo & table_info) const { return mapTableNameByID(table_info.id); }
    virtual String displayTableName(const TiDB::TableInfo & table_info) const { return table_info.name; }
    virtual String mapTableNameByID(const TiDB::TableID table_id) const
    {
        return fmt::format("{}{}", TABLE_PREFIX, table_id);
    }
    virtual String mapPartitionName(const TiDB::TableInfo & table_info) const { return mapTableName(table_info); }

    // Only use for logging / debugging
    virtual String debugDatabaseName(const TiDB::DBInfo & db_info) const { return db_info.name + "(" + std::to_string(db_info.id) + ")"; }
    virtual String debugTableName(const TiDB::TableInfo & table_info) const
    {
        return table_info.name + "(" + std::to_string(table_info.id) + ")";
    }
    virtual String debugCanonicalName(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info) const
    {
        return debugDatabaseName(db_info) + "." + debugTableName(table_info);
    }
};

} // namespace DB

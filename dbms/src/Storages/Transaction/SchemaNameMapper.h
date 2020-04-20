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
    virtual String mapTableName(const TiDB::TableInfo & table_info) const { return TABLE_PREFIX + std::to_string(table_info.id); }
    virtual String displayTableName(const TiDB::TableInfo & table_info) const { return table_info.name; }
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

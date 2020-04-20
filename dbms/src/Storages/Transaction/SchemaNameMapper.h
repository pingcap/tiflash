#pragma once

#include <Storages/Transaction/TiDB.h>

namespace DB
{

/// Always call functions in this class when trying to get the underlying entity (db/table/partition) name in CH.
struct SchemaNameMapper
{
    virtual ~SchemaNameMapper() = default;

    virtual String mapDatabaseName(const TiDB::DBInfo & db_info) const { return std::to_string(db_info.id); }
    virtual String displayDatabaseName(const TiDB::DBInfo & db_info) const { return db_info.name + "(" + std::to_string(db_info.id) + ")"; }
    virtual String mapTableName(const TiDB::TableInfo & table_info) const { return std::to_string(table_info.id); }
    virtual String displayTableName(const TiDB::TableInfo & table_info) const
    {
        return table_info.name + "(" + std::to_string(table_info.id) + ")";
    }
    virtual String displayCanonicalName(const TiDB::DBInfo & db_info, const TiDB::TableInfo & table_info) const
    {
        return displayDatabaseName(db_info) + "." + displayTableName(table_info);
    }
    virtual String mapPartitionName(const TiDB::TableInfo & table_info) const { return std::to_string(table_info.id); }
};

} // namespace DB

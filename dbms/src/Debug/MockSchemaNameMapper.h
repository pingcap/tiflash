#pragma once

#include <Storages/Transaction/SchemaNameMapper.h>

namespace DB
{

struct MockSchemaNameMapper : public SchemaNameMapper
{
    String mapDatabaseName(const TiDB::DBInfo & db_info) const override { return db_info.name; }
    String mapTableName(const TiDB::TableInfo & table_info) const override { return table_info.name; }

    String mapPartitionName(const TiDB::TableInfo & table_info) const override
    {
        return table_info.name + "_" + std::to_string(table_info.id);
    }

    String debugDatabaseName(const TiDB::DBInfo & db_info) const override { return db_info.name; }
    String debugTableName(const TiDB::TableInfo & table_info) const override { return table_info.name; }
};

} // namespace DB

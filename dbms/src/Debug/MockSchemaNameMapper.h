#pragma once

#include <Storages/Transaction/SchemaNameMapper.h>

namespace DB
{

struct MockSchemaNameMapper : public SchemaNameMapper
{
    String mapDatabaseName(const TiDB::DBInfo & db_info) const override { return db_info.name; }
    String displayDatabaseName(const TiDB::DBInfo & db_info, bool show_id=true) const override { (void)show_id; return db_info.name; }
    String mapTableName(const TiDB::TableInfo & table_info) const override { return table_info.name; }
    String displayTableName(const TiDB::TableInfo & table_info, bool show_id=true) const override { (void)show_id; return table_info.name; }
    String mapPartitionName(const TiDB::TableInfo & table_info) const override
    {
        return table_info.name + "_" + std::to_string(table_info.id);
    }
};

} // namespace DB

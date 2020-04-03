#pragma once

#include <Debug/MockTiDB.h>
#include <Storages/Transaction/SchemaGetter.h>

namespace DB
{

struct MockSchemaGetter
{

    TiDB::DBInfoPtr getDatabase(DatabaseID db_id) { return MockTiDB::instance().getDBInfoByID(db_id); }

    Int64 getVersion() { return MockTiDB::instance().getVersion(); }

    SchemaDiff getSchemaDiff(Int64 version) { return MockTiDB::instance().getSchemaDiff(version); }

    TiDB::TableInfoPtr getTableInfo(DatabaseID, TableID table_id) { return MockTiDB::instance().getTableInfoByID(table_id); }

    std::vector<TiDB::DBInfoPtr> listDBs()
    {
        std::vector<TiDB::DBInfoPtr> res;
        const auto & databases = MockTiDB::instance().getDatabases();
        for (auto it = databases.begin(); it != databases.end(); it++)
        {
            auto db_id = it->second;
            auto db_name = it->first;
            TiDB::DBInfoPtr db_ptr = std::make_shared<TiDB::DBInfo>(TiDB::DBInfo());
            db_ptr->id = db_id;
            db_ptr->name = db_name;
            res.push_back(db_ptr);
        }
        return res;
    }

    std::vector<TiDB::TableInfoPtr> listTables(Int64 db_id)
    {
        auto tables_by_id = MockTiDB::instance().getTables();
        std::vector<TiDB::TableInfoPtr> res;
        for (auto it = tables_by_id.begin(); it != tables_by_id.end(); it++)
        {
            if (it->second->dbID() == db_id)
            {
                res.push_back(std::make_shared<TiDB::TableInfo>(TiDB::TableInfo(it->second->table_info)));
            }
        }
        return res;
    }
};

} // namespace DB

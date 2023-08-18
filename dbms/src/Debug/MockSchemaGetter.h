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

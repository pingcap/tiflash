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
#include <TiDB/Schema/SchemaGetter.h>

#include <optional>

namespace DB
{
struct MockSchemaGetter
{
    static TiDB::DBInfoPtr getDatabase(DatabaseID db_id) { return MockTiDB::instance().getDBInfoByID(db_id); }

    static Int64 getVersion() { return MockTiDB::instance().getVersion(); }

    static std::optional<SchemaDiff> getSchemaDiff(Int64 version)
    {
        return MockTiDB::instance().getSchemaDiff(version);
    }

    static bool checkSchemaDiffExists(Int64 version) { return MockTiDB::instance().checkSchemaDiffExists(version); }

    static TiDB::TableInfoPtr getTableInfo(DatabaseID, TableID table_id, [[maybe_unused]] bool try_mvcc = true)
    {
        return MockTiDB::instance().getTableInfoByID(table_id);
    }

    static std::pair<TiDB::TableInfoPtr, bool> getTableInfoAndCheckMvcc(DatabaseID db_id, TableID table_id)
    {
        return {getTableInfo(db_id, table_id), false};
    }

    static std::vector<TiDB::DBInfoPtr> listDBs()
    {
        std::vector<TiDB::DBInfoPtr> res;
        const auto & databases = MockTiDB::instance().getDatabases();
        for (const auto & database : databases)
        {
            auto db_id = database.second;
            auto db_name = database.first;
            TiDB::DBInfoPtr db_ptr = std::make_shared<TiDB::DBInfo>(TiDB::DBInfo());
            db_ptr->id = db_id;
            db_ptr->name = db_name;
            res.push_back(db_ptr);
        }
        return res;
    }

    static std::vector<TiDB::TableInfoPtr> listTables(Int64 db_id)
    {
        auto tables_by_id = MockTiDB::instance().getTables();
        std::vector<TiDB::TableInfoPtr> res;
        for (auto & it : tables_by_id)
        {
            if (it.second->dbID() == db_id)
            {
                res.push_back(std::make_shared<TiDB::TableInfo>(TiDB::TableInfo(it.second->table_info)));
            }
        }
        return res;
    }

    KeyspaceID getKeyspaceID() const { return NullspaceID; }
};

} // namespace DB

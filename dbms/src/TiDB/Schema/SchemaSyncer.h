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

#include <Interpreters/Context_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>

#include <memory>
#include <vector>

namespace TiDB
{
struct DBInfo;
using DBInfoPtr = std::shared_ptr<DBInfo>;
struct TableInfo;
using TableInfoPtr = std::shared_ptr<TableInfo>;
} // namespace TiDB

namespace DB
{

class SchemaSyncer
{
public:
    virtual ~SchemaSyncer() = default;

    /*
     * Sync all tables' schemas based on schema diff, but may not apply all diffs.
     */
    virtual bool syncSchemas(Context & context) = 0;

    /*
     * Sync the table's inner schema(like add columns, modify columns, etc) for given physical_table_id
     * This function will be called concurrently when the schema not matches during reading or writing
     */
    virtual bool syncTableSchema(Context & context, TableID physical_table_id) = 0;

    virtual void reset() = 0;

    virtual TiDB::DBInfoPtr getDBInfoByName(const String & database_name) = 0;

    virtual void removeTableID(TableID table_id) = 0;

    /**
      * Drop all schema of a given keyspace.
      * When a keyspace is removed, drop all its databases and tables.
      */
    virtual void dropAllSchema(Context & context) = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

} // namespace DB

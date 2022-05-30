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

#include <Storages/Transaction/Types.h>
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
class Context;

class SchemaSyncer
{
public:
    virtual ~SchemaSyncer() = default;

    /**
     * Get current version of CH schema.
     */
    virtual Int64 getCurrentVersion() = 0;

    /**
     * Synchronize all schemas between TiDB and CH.
     * @param context
     */
    virtual bool syncSchemas(Context & context) = 0;

    virtual void reset() = 0;

    virtual TiDB::DBInfoPtr getDBInfoByName(const String & database_name) = 0;

    virtual TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) = 0;

    virtual std::vector<TiDB::DBInfoPtr> fetchAllDBs() = 0;
};

using SchemaSyncerPtr = std::shared_ptr<SchemaSyncer>;

} // namespace DB

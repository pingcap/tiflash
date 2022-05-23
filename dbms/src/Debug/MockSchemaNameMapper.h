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

#include <TiDB/Schema/SchemaNameMapper.h>

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

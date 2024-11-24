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

#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(SchemaNameMapperTest, ParseDatabaseID)
{
    SchemaNameMapper mapper;
    ASSERT_EQ(10086, *mapper.tryGetDatabaseID("db_10086"));
    ASSERT_EQ(10086, *mapper.tryGetDatabaseID("ks_100_db_10086"));
    ASSERT_EQ(10086, *mapper.tryGetDatabaseID(mapper.mapDatabaseName(10086, 100)));
    ASSERT_FALSE(mapper.tryGetDatabaseID("abcdefg"));
    ASSERT_FALSE(mapper.tryGetDatabaseID("db_"));
    ASSERT_FALSE(mapper.tryGetDatabaseID("db_abcd"));
    ASSERT_FALSE(mapper.tryGetDatabaseID("ks_100_db_"));
    ASSERT_FALSE(mapper.tryGetDatabaseID("ks_100_db_abcd"));
}

TEST(SchemaNameMapperTest, ParseTableID)
{
    SchemaNameMapper mapper;
    ASSERT_EQ(10086, *mapper.tryGetTableID("t_10086"));
    ASSERT_EQ(10086, *mapper.tryGetTableID("ks_100_t_10086"));
    {
        TiDB::TableInfo tbl_info;
        tbl_info.id = 10086;
        tbl_info.keyspace_id = 100;
        ASSERT_EQ(10086, *mapper.tryGetTableID(mapper.mapTableName(tbl_info)));
    }
    ASSERT_FALSE(mapper.tryGetTableID("abcdefg"));
    ASSERT_FALSE(mapper.tryGetTableID("t_"));
    ASSERT_FALSE(mapper.tryGetTableID("t_abcd"));
    ASSERT_FALSE(mapper.tryGetTableID("ks_100_t_"));
    ASSERT_FALSE(mapper.tryGetTableID("ks_100_t_abcd"));
}
} // namespace DB::tests

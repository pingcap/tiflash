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

#include <Debug/MockSchemaNameMapper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Logger.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/registerStorages.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaSyncer.h>


using TableInfo = TiDB::TableInfo;
using DBInfo = TiDB::DBInfo;


namespace DB
{

String createTableStmt(const DBInfo & db_info, const TableInfo & table_info, const SchemaNameMapper & name_mapper, const LoggerPtr & log);

namespace tests
{

struct ParseCase
{
    String table_info_json;
    std::function<void(const TableInfo & table_info)> check;
};

TEST(TiDBTableInfoTest, ParseFromJSON)
try
{
    auto cases = {
        // Test for backward compatibility
        ParseCase{
            R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"t","O":"t"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":45,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":23,"state":5,"update_timestamp":418700409204899851})json",
            [](const TableInfo & table_info) {
                ASSERT_EQ(table_info.name, "t");
                ASSERT_EQ(table_info.id, 45L);
            }},
        // Test with tiflash_replica information
        ParseCase{
            R"json({"id":45,"name":{"O":"t","L":"t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"t","L":"t"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":false,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":1,"max_idx_id":0,"max_cst_id":0,"update_timestamp":418683341902184450,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":3,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null}})json",
            [](const TableInfo & table_info) {
                ASSERT_EQ(table_info.name, "t");
                ASSERT_EQ(table_info.id, 45L);
            }},
        // Test binary default value not trimmed by leading zero bytes and padded with trailing zero bytes.
        ParseCase{
            R"json({"id":45,"name":{"O":"t","L":"t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"t","L":"t"},"offset":0,"origin_default":"\u0000\u00124","origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":129,"Flen":4,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":false,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":1,"max_idx_id":0,"max_cst_id":0,"update_timestamp":418683341902184450,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":3,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null}})json",
            [](const TableInfo & table_info) {
                ASSERT_EQ(table_info.columns[0].defaultValueToField().get<String>(),
                          Field(String("\0\x12"
                                       "4\0",
                                       4))
                              .get<String>());
            }},
        // Test binary default value with exact length having the full content.
        ParseCase{
            R"json({"id":45,"name":{"O":"t","L":"t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"t","L":"t"},"offset":0,"origin_default":"\u0000\u00124","origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":129,"Flen":3,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":null,"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":false,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":1,"max_idx_id":0,"max_cst_id":0,"update_timestamp":418683341902184450,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":3,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null}})json",
            [](const TableInfo & table_info) {
                ASSERT_EQ(table_info.columns[0].defaultValueToField().get<String>(),
                          Field(String("\0\x12"
                                       "4",
                                       3))
                              .get<String>());
            }},
    };

    for (const auto & c : cases)
    {
        TableInfo table_info(c.table_info_json, NullspaceID);
        c.check(table_info);
    }
}
CATCH

struct StmtCase
{
    TableID table_or_partition_id;
    String db_info_json;
    String table_info_json;
    String create_stmt_dm;

    void verifyTableInfo() const
    {
        DBInfo db_info(db_info_json, NullspaceID);
        TableInfo table_info(table_info_json, NullspaceID);
        if (table_info.is_partition_table)
            table_info = *table_info.producePartitionTableInfo(table_or_partition_id, MockSchemaNameMapper());
        auto json1 = table_info.serialize();
        TableInfo table_info2(json1, NullspaceID);
        auto json2 = table_info2.serialize();
        EXPECT_EQ(json1, json2) << "Table info unescaped serde mismatch:\n" + json1 + "\n" + json2;

        // generate create statement with db_info and table_info
        auto verify_stmt = [&](TiDB::StorageEngine engine_type) {
            table_info.engine_type = engine_type;
            String stmt = createTableStmt(db_info, table_info, MockSchemaNameMapper(), Logger::get());
            EXPECT_EQ(stmt, create_stmt_dm) << "Table info create statement mismatch:\n" + stmt + "\n" + create_stmt_dm;

            json1 = extractTableInfoFromCreateStatement(stmt, table_info.name);
            table_info.deserialize(json1);
            json2 = table_info.serialize();
            EXPECT_EQ(json1, json2) << "Table info escaped serde mismatch:\n" + json1 + "\n" + json2;
        };

        verify_stmt(TiDB::StorageEngine::DT);
    }

private:
    static String extractTableInfoFromCreateStatement(const String & stmt, const String & tbl_name)
    {
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from verifyTableInfo " + tbl_name, 0);
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ASTExpressionList & ast_arguments = typeid_cast<ASTExpressionList &>(*(ast_create_query.storage->engine->arguments));
        ASTLiteral & ast_literal = typeid_cast<ASTLiteral &>(*(ast_arguments.children.back()));
        return safeGet<String>(ast_literal.value);
    }
};

TEST(TiDBTableInfoTest, GenCreateTableStatement)
try
{
    auto cases = //
        {StmtCase{
             1145, //
             R"json({"id":1939,"db_name":{"O":"customer","L":"customer"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":1145,"name":{"O":"customerdebt","L":"customerdebt"},"cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"type":{"Tp":8,"Flag":515,"Flen":20,"Decimal":0},"state":5,"comment":"i\"d"}],"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"负债信息","partition":null})json", //
             R"stmt(CREATE TABLE `customer`.`customerdebt`(`id` Int64) Engine = DeltaMerge((`id`), '{"cols":[{"comment":"i\\"d","default":null,"default_bit":null,"id":1,"name":{"L":"id","O":"id"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":515,"Flen":20,"Tp":8}}],"comment":"负债信息","id":1145,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"customerdebt","O":"customerdebt"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":0}'))stmt", //
         },
         StmtCase{
             2049, //
             R"json({"id":1939,"db_name":{"O":"customer","L":"customer"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":2049,"name":{"O":"customerdebt","L":"customerdebt"},"cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"type":{"Tp":8,"Flag":515,"Flen":20,"Decimal":0},"state":5,"comment":"i\"d"}],"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"负债信息","update_timestamp":404545295996944390,"partition":null})json", //
             R"stmt(CREATE TABLE `customer`.`customerdebt`(`id` Int64) Engine = DeltaMerge((`id`), '{"cols":[{"comment":"i\\"d","default":null,"default_bit":null,"id":1,"name":{"L":"id","O":"id"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":515,"Flen":20,"Tp":8}}],"comment":"负债信息","id":2049,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"customerdebt","O":"customerdebt"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":404545295996944390}'))stmt", //
         },
         StmtCase{
             31, //
             R"json({"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":31,"name":{"O":"simple_t","L":"simple_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545295996944390,"ShardRowIDBits":0,"partition":null})json", //
             R"stmt(CREATE TABLE `db1`.`simple_t`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = DeltaMerge((`_tidb_rowid`), '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":31,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"simple_t","O":"simple_t"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":404545295996944390}'))stmt", //
         },
         StmtCase{
             33, //
             R"json({"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":33,"name":{"O":"pk_t","L":"pk_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545312978108418,"ShardRowIDBits":0,"partition":null})json", //
             R"stmt(CREATE TABLE `db2`.`pk_t`(`i` Int32) Engine = DeltaMerge((`i`), '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":3,"Flen":11,"Tp":3}}],"comment":"","id":33,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"pk_t","O":"pk_t"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":404545312978108418}'))stmt", //
         },
         StmtCase{
             35, //
             R"json({"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":35,"name":{"O":"not_null_t","L":"not_null_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4097,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545324922961926,"ShardRowIDBits":0,"partition":null})json", //
             R"stmt(CREATE TABLE `db1`.`not_null_t`(`i` Int32, `_tidb_rowid` Int64) Engine = DeltaMerge((`_tidb_rowid`), '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":4097,"Flen":11,"Tp":3}}],"comment":"","id":35,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"not_null_t","O":"not_null_t"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":404545324922961926}'))stmt", //
         },
         StmtCase{
             37, //
             R"json({"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
             R"json({"id":37,"name":{"O":"mytable","L":"mytable"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"mycol","L":"mycol"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":4099,"Flen":256,"Decimal":0,"Charset":"utf8","Collate":"utf8_bin","Elems":null},"state":5,"comment":""}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"mycol","L":"mycol"},"offset":0,"length":-1}],"is_unique":true,"is_primary":true,"state":5,"comment":"","index_type":1}],"fk_info":null,"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":1,"update_timestamp":404566455285710853,"ShardRowIDBits":0,"partition":null})json", //
             R"stmt(CREATE TABLE `db2`.`mytable`(`mycol` String) Engine = DeltaMerge((`mycol`), '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"mycol","O":"mycol"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8","Collate":"utf8_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":256,"Tp":15}}],"comment":"","id":37,"index_info":[],"is_common_handle":false,"keyspace_id":4294967295,"name":{"L":"mytable","O":"mytable"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":404566455285710853}'))stmt", //
         },
         StmtCase{
             32, //
             R"json({"id":1,"db_name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json", //
             R"json({"id":31,"name":{"O":"range_part_t","L":"range_part_t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","version":0}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":407445773801488390,"ShardRowIDBits":0,"partition":{"type":1,"expr":"`i`","columns":null,"enable":true,"definitions":[{"id":32,"name":{"O":"p0","L":"p0"},"less_than":["0"]},{"id":33,"name":{"O":"p1","L":"p1"},"less_than":["100"]}],"num":0},"compression":"","version":1})json", //
             R"stmt(CREATE TABLE `test`.`range_part_t_32`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = DeltaMerge((`_tidb_rowid`), '{"belonging_table_id":31,"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":32,"index_info":[],"is_common_handle":false,"is_partition_sub_table":true,"keyspace_id":4294967295,"name":{"L":"range_part_t_32","O":"range_part_t_32"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":407445773801488390}'))stmt", //
         }};

    for (const auto & c : cases)
    {
        c.verifyTableInfo();
    }
}
CATCH

} // namespace tests
} // namespace DB

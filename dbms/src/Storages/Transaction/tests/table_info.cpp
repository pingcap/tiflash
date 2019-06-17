#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/registerStorages.h>


using TableInfo = TiDB::TableInfo;
using namespace DB;


namespace DB
{

String createTableStmt(const TableInfo & table_info);

}

struct Case
{
    TableID table_or_partition_id;
    String table_info_json;
    String create_stmt;

    void verifyTableInfo() const
    {
        TableInfo table_info1(table_info_json, false);
        table_info1.manglePartitionTableIfNeeded(table_or_partition_id);
        auto json1 = table_info1.serialize(false);
        TableInfo table_info2(json1, false);
        auto json2 = table_info2.serialize(false);
        if (json1 != json2)
        {
            throw Exception("Table info unescaped serde mismatch:\n" + json1 + "\n" + json2);
        }
        String stmt = createTableStmt(table_info1);
        if (stmt != create_stmt)
        {
            throw Exception("Table info create statement mismatch:\n" + stmt + "\n" + create_stmt);
        }

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from verifyTableInfo " + table_info1.name, 0);
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ASTExpressionList & ast_arguments = typeid_cast<ASTExpressionList &>(*(ast_create_query.storage->engine->arguments));
        ASTLiteral & ast_literal = typeid_cast<ASTLiteral &>(*(ast_arguments.children.back()));
        json1 = safeGet<String>(ast_literal.value);
        table_info1.deserialize(json1, true);
        json2 = table_info1.serialize(true);
        if (json1 != json2)
        {
            throw Exception("Table info escaped serde mismatch:\n" + json1 + "\n" + json2);
        }
    }
};

int main(int, char **)
{
    auto cases =
    {
        Case
        {
            31,
            R"json({"db_info":{"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5},"table_info":{"id":31,"name":{"O":"simple_t","L":"simple_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545295996944390,"ShardRowIDBits":0,"partition":null},"schema_version":100})json",
            R"stmt(CREATE TABLE `db1`.`simple_t`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"db_info":{"id":1,"db_name":{"O":"db1","L":"db1"}},"table_info":{"id":31,"name":{"O":"simple_t","L":"simple_t"},"cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0},"state":5,"comment":""}],"state":5,"pk_is_handle":false,"comment":"","update_timestamp":404545295996944390,"partition":null},"schema_version":100}'))stmt"
        },
        Case
        {
            33,
            R"json({"db_info":{"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5},"table_info":{"id":33,"name":{"O":"pk_t","L":"pk_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545312978108418,"ShardRowIDBits":0,"partition":null},"schema_version":101})json",
            R"stmt(CREATE TABLE `db2`.`pk_t`(`i` Int32) Engine = TxnMergeTree((`i`), 8192, '{"db_info":{"id":2,"db_name":{"O":"db2","L":"db2"}},"table_info":{"id":33,"name":{"O":"pk_t","L":"pk_t"},"cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0},"state":5,"comment":""}],"state":5,"pk_is_handle":true,"comment":"","update_timestamp":404545312978108418,"partition":null},"schema_version":101}'))stmt"
        },
        Case
        {
            35,
            R"json({"db_info":{"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5},"table_info":{"id":35,"name":{"O":"not_null_t","L":"not_null_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4097,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545324922961926,"ShardRowIDBits":0,"partition":null},"schema_version":102})json",
            R"stmt(CREATE TABLE `db1`.`not_null_t`(`i` Int32, `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"db_info":{"id":1,"db_name":{"O":"db1","L":"db1"}},"table_info":{"id":35,"name":{"O":"not_null_t","L":"not_null_t"},"cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":3,"Flag":4097,"Flen":11,"Decimal":0},"state":5,"comment":""}],"state":5,"pk_is_handle":false,"comment":"","update_timestamp":404545324922961926,"partition":null},"schema_version":102}'))stmt"
        },
        Case
        {
            37,
            R"json({"db_info":{"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5},"table_info":{"id":37,"name":{"O":"mytable","L":"mytable"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"mycol","L":"mycol"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":4099,"Flen":256,"Decimal":0,"Charset":"utf8","Collate":"utf8_bin","Elems":null},"state":5,"comment":""}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"mycol","L":"mycol"},"offset":0,"length":-1}],"is_unique":true,"is_primary":true,"state":5,"comment":"","index_type":1}],"fk_info":null,"state":5,"pk_is_handle":true,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":1,"update_timestamp":404566455285710853,"ShardRowIDBits":0,"partition":null},"schema_version":103})json",
            R"stmt(CREATE TABLE `db2`.`mytable`(`mycol` String) Engine = TxnMergeTree((`mycol`), 8192, '{"db_info":{"id":2,"db_name":{"O":"db2","L":"db2"}},"table_info":{"id":37,"name":{"O":"mytable","L":"mytable"},"cols":[{"id":1,"name":{"O":"mycol","L":"mycol"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":15,"Flag":4099,"Flen":256,"Decimal":0},"state":5,"comment":""}],"state":5,"pk_is_handle":true,"comment":"","update_timestamp":404566455285710853,"partition":null},"schema_version":103}'))stmt"
        },
        Case
        {
            32,
            R"json({"db_info":{"id":1,"db_name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5},"table_info":{"id":31,"name":{"O":"range_part_t","L":"range_part_t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","version":0}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":407445773801488390,"ShardRowIDBits":0,"partition":{"type":1,"expr":"`i`","columns":null,"enable":true,"definitions":[{"id":32,"name":{"O":"p0","L":"p0"},"less_than":["0"]},{"id":33,"name":{"O":"p1","L":"p1"},"less_than":["100"]}],"num":0},"compression":"","version":1},"schema_version":16})json",
            R"stmt(CREATE TABLE `test`.`range_part_t_32`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"db_info":{"id":1,"db_name":{"O":"test","L":"test"}},"table_info":{"id":32,"name":{"O":"range_part_t_32","L":"range_part_t_32"},"cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0},"state":5,"comment":""}],"state":5,"pk_is_handle":false,"comment":"","update_timestamp":407445773801488390,"belonging_table_id":31,"is_partition_sub_table":true,"partition":{"type":1,"expr":"`i`","enable":true,"definitions":[{"id":32,"name":{"O":"p0","L":"p0"},"comment":""},{"id":33,"name":{"O":"p1","L":"p1"},"comment":""}],"num":0}},"schema_version":16}'))stmt"
        }
    };

    for (auto & c : cases)
    {
        c.verifyTableInfo();
    }

    return 0;
}

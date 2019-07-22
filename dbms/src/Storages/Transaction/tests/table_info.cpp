#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/registerStorages.h>


using TableInfo = TiDB::TableInfo;
using DBInfo = TiDB::DBInfo;
using namespace DB;


namespace DB
{

String createTableStmt(const DBInfo & db_info, const TableInfo & table_info);

}

struct Case
{
    TableID table_or_partition_id;
    String db_info_json;
    String table_info_json;
    String create_stmt;

    void verifyTableInfo() const
    {
        DBInfo db_info(db_info_json);
        TableInfo table_info(table_info_json);
        if (table_info.is_partition_table)
            table_info = table_info.producePartitionTableInfo(table_or_partition_id);
        auto json1 = table_info.serialize(false);
        TableInfo table_info2(json1);
        auto json2 = table_info2.serialize(false);
        if (json1 != json2)
        {
            throw Exception("Table info unescaped serde mismatch:\n" + json1 + "\n" + json2);
        }
        String stmt = createTableStmt(db_info, table_info);
        if (stmt != create_stmt)
        {
            throw Exception("Table info create statement mismatch:\n" + stmt + "\n" + create_stmt);
        }

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from verifyTableInfo " + table_info.name, 0);
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ASTExpressionList & ast_arguments = typeid_cast<ASTExpressionList &>(*(ast_create_query.storage->engine->arguments));
        ASTLiteral & ast_literal = typeid_cast<ASTLiteral &>(*(ast_arguments.children.back()));
        json1 = safeGet<String>(ast_literal.value);
        table_info.deserialize(json1);
        json2 = table_info.serialize(false);
        if (json1 != json2)
        {
            throw Exception("Table info escaped serde mismatch:\n" + json1 + "\n" + json2);
        }
    }
};

int main(int, char **) try
{
    auto cases = {
        Case{
            2049,
            R"json({"id":1939,"db_name":{"O":"customer","L":"customer"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":2049,"name":{"O":"customerdebt","L":"customerdebt"},"cols":[{"id":1,"name":{"O":"id","L":"id"},"offset":0,"origin_default":null,"default":null,"type":{"Tp":8,"Flag":515,"Flen":20,"Decimal":0},"state":5,"comment":"i\"d"}],"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"负债信息","update_timestamp":404545295996944390,"partition":null})json",
            R"stmt(CREATE TABLE `customer`.`customerdebt`(`id` Int64) Engine = TxnMergeTree((`id`), 8192, '{"cols":[{"comment":"i\\"d","default":null,"id":1,"name":{"L":"id","O":"id"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":515,"Flen":20,"Tp":8}}],"comment":"\\u8D1F\\u503A\\u4FE1\\u606F","id":2049,"name":{"L":"customerdebt","O":"customerdebt"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"update_timestamp":404545295996944390}'))stmt"
        },
        Case
        {
            31,
            R"json({"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":31,"name":{"O":"simple_t","L":"simple_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545295996944390,"ShardRowIDBits":0,"partition":null})json",
            R"stmt(CREATE TABLE `db1`.`simple_t`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":31,"name":{"L":"simple_t","O":"simple_t"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"update_timestamp":404545295996944390}'))stmt"
        },
        Case
        {
            33,
            R"json({"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":33,"name":{"O":"pk_t","L":"pk_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545312978108418,"ShardRowIDBits":0,"partition":null})json",
            R"stmt(CREATE TABLE `db2`.`pk_t`(`i` Int32) Engine = TxnMergeTree((`i`), 8192, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":3,"Flen":11,"Tp":3}}],"comment":"","id":33,"name":{"L":"pk_t","O":"pk_t"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"update_timestamp":404545312978108418}'))stmt"
        },
        Case
        {
            35,
            R"json({"id":1,"db_name":{"O":"db1","L":"db1"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":35,"name":{"O":"not_null_t","L":"not_null_t"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4097,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":""}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":404545324922961926,"ShardRowIDBits":0,"partition":null})json",
            R"stmt(CREATE TABLE `db1`.`not_null_t`(`i` Int32, `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4097,"Flen":11,"Tp":3}}],"comment":"","id":35,"name":{"L":"not_null_t","O":"not_null_t"},"partition":null,"pk_is_handle":false,"schema_version":-1,"state":5,"update_timestamp":404545324922961926}'))stmt"
        },
        Case
        {
            37,
            R"json({"id":2,"db_name":{"O":"db2","L":"db2"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":37,"name":{"O":"mytable","L":"mytable"},"charset":"","collate":"","cols":[{"id":1,"name":{"O":"mycol","L":"mycol"},"offset":0,"origin_default":null,"default":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":4099,"Flen":256,"Decimal":0,"Charset":"utf8","Collate":"utf8_bin","Elems":null},"state":5,"comment":""}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"mycol","L":"mycol"},"offset":0,"length":-1}],"is_unique":true,"is_primary":true,"state":5,"comment":"","index_type":1}],"fk_info":null,"state":5,"pk_is_handle":true,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":1,"update_timestamp":404566455285710853,"ShardRowIDBits":0,"partition":null})json",
            R"stmt(CREATE TABLE `db2`.`mytable`(`mycol` String) Engine = TxnMergeTree((`mycol`), 8192, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"mycol","O":"mycol"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4099,"Flen":256,"Tp":15}}],"comment":"","id":37,"name":{"L":"mytable","O":"mytable"},"partition":null,"pk_is_handle":true,"schema_version":-1,"state":5,"update_timestamp":404566455285710853}'))stmt"
        },
        Case
        {
            32,
            R"json({"id":1,"db_name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","state":5})json",
            R"json({"id":31,"name":{"O":"range_part_t","L":"range_part_t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"i","L":"i"},"offset":0,"origin_default":null,"default":null,"default_bit":null,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","version":0}],"index_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"schema_version":-1,"comment":"","auto_inc_id":0,"max_col_id":1,"max_idx_id":0,"update_timestamp":407445773801488390,"ShardRowIDBits":0,"partition":{"type":1,"expr":"`i`","columns":null,"enable":true,"definitions":[{"id":32,"name":{"O":"p0","L":"p0"},"less_than":["0"]},{"id":33,"name":{"O":"p1","L":"p1"},"less_than":["100"]}],"num":0},"compression":"","version":1})json",
            R"stmt(CREATE TABLE `test`.`range_part_t_32`(`i` Nullable(Int32), `_tidb_rowid` Int64) Engine = TxnMergeTree((`_tidb_rowid`), 8192, '{"belonging_table_id":31,"cols":[{"comment":"","default":null,"id":1,"name":{"L":"i","O":"i"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":32,"is_partition_sub_table":true,"name":{"L":"range_part_t_32","O":"range_part_t_32"},"partition":{"definitions":[{"comment":"","id":32,"name":{"L":"p0","O":"p0"}},{"comment":"","id":33,"name":{"L":"p1","O":"p1"}}],"enable":true,"expr":"`i`","num":0,"type":1},"pk_is_handle":false,"schema_version":-1,"state":5,"update_timestamp":407445773801488390}'))stmt"
        }
    };

    for (auto & c : cases)
    {
        c.verifyTableInfo();
    }

    return 0;
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << std::endl;
}

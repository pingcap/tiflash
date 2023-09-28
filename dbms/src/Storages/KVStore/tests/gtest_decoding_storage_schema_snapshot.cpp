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

#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/tests/RowCodecTestUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{
static TableInfo getTableInfoByJson(const String & json_table_info)
{
    return TableInfo(json_table_info, NullspaceID);
}
TEST(DecodingStorageSchemaSnapshotTest, CheckPKInfosUnderClusteredIndex)
{
    // table with column [A,B,C,D], primary keys [A,C]
    const String json_table_info
        = R"json({"id":75,"name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"A","L":"a"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"B","L":"b"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":20,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":3,"name":{"O":"C","L":"c"},"offset":2,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":4,"name":{"O":"D","L":"d"},"offset":3,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"A","L":"a"},"offset":0,"length":-1},{"name":{"O":"C","L":"c"},"offset":2,"length":-1}],"state":5,"comment":"","index_type":1,"is_unique":true,"is_primary":true,"is_invisible":false,"is_global":false}],"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":true,"common_handle_version":1,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":4,"max_idx_id":1,"max_cst_id":0,"update_timestamp":434039123413303302,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":4,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null},"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null})json";
    auto table_info = getTableInfoByJson(json_table_info);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    //check decoding_schema->pk_column_ids infos
    ASSERT_EQ(decoding_schema->pk_column_ids.size(), 2);
    ASSERT_EQ(decoding_schema->pk_column_ids[0], 1);
    ASSERT_EQ(decoding_schema->pk_column_ids[1], 3);

    //check decoding_schema->pk_pos_map infos
    ASSERT_EQ(decoding_schema->pk_column_ids.size(), decoding_schema->pk_pos_map.size());
    // there are three hidden column in the decoded block, so the position of A,C is 3,5
    ASSERT_EQ(decoding_schema->pk_pos_map.at(decoding_schema->pk_column_ids[0]), 3);
    ASSERT_EQ(decoding_schema->pk_pos_map.at(decoding_schema->pk_column_ids[1]), 5);
}

TEST(DecodingStorageSchemaSnapshotTest, CheckPKInfosUnderClusteredIndexAfterDropColumn)
{
    // drop column B for [A,B,C,D]; table with column [A,C,D], primary keys [A,C]
    const String json_table_info
        = R"json({"id":75,"name":{"O":"test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"A","L":"a"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":3,"name":{"O":"C","L":"c"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":4,"name":{"O":"D","L":"d"},"offset":2,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"A","L":"a"},"offset":0,"length":-1},{"name":{"O":"C","L":"c"},"offset":1,"length":-1}],"state":5,"comment":"","index_type":1,"is_unique":true,"is_primary":true,"is_invisible":false,"is_global":false}],"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":false,"is_common_handle":true,"common_handle_version":1,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":4,"max_idx_id":1,"max_cst_id":0,"update_timestamp":434039123413303302,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":4,"tiflash_replica":{"Count":1,"LocationLabels":[],"Available":false,"AvailablePartitionIDs":null},"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null})json";
    auto table_info = getTableInfoByJson(json_table_info);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    //check decoding_schema->pk_column_ids infos
    ASSERT_EQ(decoding_schema->pk_column_ids.size(), 2);
    ASSERT_EQ(decoding_schema->pk_column_ids[0], 1);
    ASSERT_EQ(decoding_schema->pk_column_ids[1], 3);

    //check decoding_schema->pk_pos_map infos
    ASSERT_EQ(decoding_schema->pk_column_ids.size(), decoding_schema->pk_pos_map.size());
    // there are three hidden column in the decoded block, so the position of A,C is 3,4
    ASSERT_EQ(decoding_schema->pk_pos_map.at(decoding_schema->pk_column_ids[0]), 3);
    ASSERT_EQ(decoding_schema->pk_pos_map.at(decoding_schema->pk_column_ids[1]), 4);
}

} // namespace DB::tests

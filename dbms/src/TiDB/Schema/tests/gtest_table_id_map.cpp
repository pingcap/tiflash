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

#include <Storages/KVStore/Types.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/TableIDMap.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-internal.h>

namespace DB::tests
{
::testing::AssertionResult isSameMapping(
    const char * lhs_expr,
    const char * rhs_expr,
    std::tuple<bool, DatabaseID, TableID> lhs,
    std::tuple<bool, DatabaseID, TableID> rhs)
{
    if (std::get<0>(lhs) == std::get<0>(rhs))
    {
        // not found
        if (!std::get<0>(lhs))
            return ::testing::AssertionSuccess();
        // the mapping is found, compare the database_id -> table_id
        if (std::get<1>(lhs) == std::get<1>(rhs) //
            && std::get<2>(lhs) == std::get<2>(rhs))
            return ::testing::AssertionSuccess();
    }
    return ::testing::internal::EqFailure(
        lhs_expr,
        rhs_expr,
        fmt::format("<found={} db_id={} tbl_id={}>", std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs)),
        fmt::format("<found={} db_id={} tbl_id={}>", std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs)),
        false);
}

#define ASSERT_MAPPING_EQ(val1, val2) ASSERT_PRED_FORMAT2(isSameMapping, val1, val2)

class TableIDMapTest : public ::testing::Test
{
public:
    TableIDMapTest()
        : log(Logger::get())
    {}


protected:
    LoggerPtr log;
};

TEST_F(TableIDMapTest, Basic)
{
    TableIDMap mapping(log);

    // partition table
    TableID partition_logical_table_id = 100;
    mapping.emplaceTableID(partition_logical_table_id, 2);
    mapping.emplacePartitionTableID(101, 100);
    mapping.emplacePartitionTableID(102, 100);
    mapping.emplacePartitionTableID(103, 100);
    // non-partition table
    TableID non_partition_table_id = 200;
    mapping.emplaceTableID(non_partition_table_id, 2);

    ASSERT_EQ(mapping.findTableIDInDatabaseMap(partition_logical_table_id), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(101), partition_logical_table_id);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), -1);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(non_partition_table_id), -1);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(non_partition_table_id), 2);

    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(101));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 200), mapping.findDatabaseIDAndLogicalTableID(200));

    // non exist
    ASSERT_MAPPING_EQ(std::make_tuple(false, 0, 0), mapping.findDatabaseIDAndLogicalTableID(900));
    // broken state, physical_table_id -> logical_table_id, but no logical_table_id -> database_id
    mapping.emplacePartitionTableID(901, 900);
    ASSERT_MAPPING_EQ(std::make_tuple(false, 0, 0), mapping.findDatabaseIDAndLogicalTableID(901));

    const auto p_to_db = mapping.getAllPartitionsBelongDatabase();
    EXPECT_EQ(p_to_db.size(), 3);
    EXPECT_EQ(p_to_db.at(101), 2);
    EXPECT_EQ(p_to_db.at(102), 2);
    EXPECT_EQ(p_to_db.at(103), 2);
}

TEST_F(TableIDMapTest, ExchangePartition)
{
    TableIDMap mapping(log);

    /// Prepare
    // partition table
    TableID partition_logical_table_id = 100;
    mapping.emplaceTableID(partition_logical_table_id, 2);
    mapping.emplacePartitionTableID(101, 100);
    mapping.emplacePartitionTableID(102, 100);
    mapping.emplacePartitionTableID(103, 100);
    // non-partition table
    TableID non_partition_table_id = 200;
    mapping.emplaceTableID(non_partition_table_id, 2);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(partition_logical_table_id), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(101), partition_logical_table_id);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), -1);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(non_partition_table_id), -1);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(non_partition_table_id), 2);
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(101));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 200), mapping.findDatabaseIDAndLogicalTableID(200));

    {
        // the mapping of partition_id to database before exchange
        const auto p_to_db = mapping.getAllPartitionsBelongDatabase();
        EXPECT_EQ(p_to_db.size(), 3);
        EXPECT_EQ(p_to_db.at(101), 2);
        EXPECT_EQ(p_to_db.at(102), 2);
        EXPECT_EQ(p_to_db.at(103), 2);
    }

    // exchange
    mapping.exchangeTablePartition(2, non_partition_table_id, 2, partition_logical_table_id, 101);

    ASSERT_EQ(mapping.findTableIDInDatabaseMap(partition_logical_table_id), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(101), -1);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(non_partition_table_id), partition_logical_table_id);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(non_partition_table_id), -1);
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 101), mapping.findDatabaseIDAndLogicalTableID(101)); // changed
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(200)); // changed

    {
        // the mapping of partition_id to database before exchange
        const auto p_to_db = mapping.getAllPartitionsBelongDatabase();
        EXPECT_EQ(p_to_db.size(), 3);
        EXPECT_EQ(p_to_db.at(non_partition_table_id), 2);
        EXPECT_EQ(p_to_db.at(102), 2);
        EXPECT_EQ(p_to_db.at(103), 2);
    }
}

TEST_F(TableIDMapTest, ExchangePartitionCrossDatabase)
{
    TableIDMap mapping(log);

    /// Prepare
    // partition table
    TableID partition_logical_table_id = 100;
    mapping.emplaceTableID(partition_logical_table_id, 2);
    mapping.emplacePartitionTableID(101, 100);
    mapping.emplacePartitionTableID(102, 100);
    mapping.emplacePartitionTableID(103, 100);
    // non-partition table
    TableID non_partition_table_id = 200;
    mapping.emplaceTableID(non_partition_table_id, 7);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(partition_logical_table_id), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(101), partition_logical_table_id);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), -1);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(non_partition_table_id), -1);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(non_partition_table_id), 7);
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(101));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 7, 200), mapping.findDatabaseIDAndLogicalTableID(200));

    {
        // the mapping of partition_id to database before exchange
        const auto p_to_db = mapping.getAllPartitionsBelongDatabase();
        EXPECT_EQ(p_to_db.size(), 3);
        EXPECT_EQ(p_to_db.at(101), 2);
        EXPECT_EQ(p_to_db.at(102), 2);
        EXPECT_EQ(p_to_db.at(103), 2);
    }

    // exchange
    mapping.exchangeTablePartition(7, non_partition_table_id, 2, partition_logical_table_id, 101);

    ASSERT_EQ(mapping.findTableIDInDatabaseMap(partition_logical_table_id), 2);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(101), -1);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), 7);
    ASSERT_EQ(mapping.findTableIDInPartitionMap(non_partition_table_id), partition_logical_table_id);
    ASSERT_EQ(mapping.findTableIDInDatabaseMap(non_partition_table_id), -1);
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 7, 101), mapping.findDatabaseIDAndLogicalTableID(101)); // changed
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
    ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(200)); // changed

    {
        // the mapping of partition_id to database before exchange
        const auto p_to_db = mapping.getAllPartitionsBelongDatabase();
        EXPECT_EQ(p_to_db.size(), 3);
        EXPECT_EQ(p_to_db.at(non_partition_table_id), 2);
        EXPECT_EQ(p_to_db.at(102), 2);
        EXPECT_EQ(p_to_db.at(103), 2);
    }
}

TEST_F(TableIDMapTest, Erase)
{
    TableIDMap mapping(log);
    {
        /// Prepare
        // partition table
        TableID partition_logical_table_id = 100;
        mapping.emplaceTableID(partition_logical_table_id, 2);
        mapping.emplacePartitionTableID(101, 100);
        mapping.emplacePartitionTableID(102, 100);
        mapping.emplacePartitionTableID(103, 100);
        // non-partition table
        TableID non_partition_table_id = 200;
        mapping.emplaceTableID(non_partition_table_id, 7);

        ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(100));
        ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(101));
        ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(102));
        ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(103));
        ASSERT_MAPPING_EQ(std::make_tuple(true, 7, 200), mapping.findDatabaseIDAndLogicalTableID(200));

        mapping.erase(101);
        ASSERT_MAPPING_EQ(std::make_tuple(false, 0, 0), mapping.findDatabaseIDAndLogicalTableID(101));
    }

    mapping.clear();

    {
        /// Prepare, assume that one table_id is inserted into two mapping
        // partition table
        TableID partition_logical_table_id = 100;
        mapping.emplaceTableID(partition_logical_table_id, 2);
        mapping.emplacePartitionTableID(101, 100);
        // non-partition table
        mapping.emplaceTableID(101, 7);
        ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), 7);

        mapping.eraseFromTableIDToDatabaseID(101);
        ASSERT_MAPPING_EQ(std::make_tuple(true, 2, 100), mapping.findDatabaseIDAndLogicalTableID(101));
        ASSERT_EQ(mapping.findTableIDInDatabaseMap(101), -1);
    }
}

} // namespace DB::tests

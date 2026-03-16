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

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Join.h>
#include <gtest/gtest.h>

#include <optional>

namespace DB::tests
{
namespace
{
constexpr auto * test_key_name = "k";
constexpr auto * probe_eq_key_name = "eq_k";
constexpr auto * probe_null_eq_key_name = "null_eq_k";
constexpr auto * probe_filter_name = "filter";

Block makeSampleBlock(const DataTypePtr & key_type)
{
    return Block{{key_type->createColumn(), key_type, test_key_name}};
}

JoinPtr makeTestJoin(const DataTypePtr & key_type, const std::vector<UInt8> & is_null_eq)
{
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{test_key_name},
        Names{test_key_name},
        is_null_eq,
        ASTTableJoin::Kind::Inner,
        "join_null_eq_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{{test_key_name, key_type}},
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true);
}

ColumnPtr makeNullableInt32Column(std::initializer_list<std::optional<Int32>> values)
{
    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    nested->reserve(values.size());
    null_map->reserve(values.size());
    auto & nested_data = nested->getData();
    auto & null_map_data = null_map->getData();
    for (const auto & value : values)
    {
        if (value.has_value())
        {
            nested_data.push_back(*value);
            null_map_data.push_back(0);
        }
        else
        {
            nested_data.push_back(0);
            null_map_data.push_back(1);
        }
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr makeUInt8Column(std::initializer_list<UInt8> values)
{
    auto column = ColumnUInt8::create();
    column->reserve(values.size());
    auto & data = column->getData();
    for (auto value : values)
        data.push_back(value);
    return column;
}
} // namespace

TEST(JoinNullEqTest, NullableNullEqKeyForcesSerializedJoinMapMethod)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto join = makeTestJoin(nullable_int_type, {1});
    join->initBuild(makeSampleBlock(nullable_int_type), 1);

    ASSERT_EQ(join->getJoinMapMethod(), JoinMapMethod::serialized);
}

TEST(JoinNullEqTest, DefaultMethodSelectionRemainsForOtherCases)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto nullable_non_null_eq_join = makeTestJoin(nullable_int_type, {0});
    nullable_non_null_eq_join->initBuild(makeSampleBlock(nullable_int_type), 1);
    ASSERT_EQ(nullable_non_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);

    auto int_type = std::make_shared<DataTypeInt32>();
    auto non_nullable_null_eq_join = makeTestJoin(int_type, {1});
    non_nullable_null_eq_join->initBuild(makeSampleBlock(int_type), 1);
    ASSERT_EQ(non_nullable_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);
}

TEST(JoinNullEqTest, NullableNullEqBuildRowsAreInsertedIntoHashMap)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto join = makeTestJoin(nullable_int_type, {1});
    join->initBuild(makeSampleBlock(nullable_int_type), 1);

    Block build_block{
        {makeNullableInt32Column({std::nullopt, 7}), nullable_int_type, test_key_name},
    };
    join->insertFromBlock(build_block, 0);

    ASSERT_EQ(join->getTotalRowCount(), 2);
}

TEST(JoinNullEqTest, ProbeRowFilterSkipsOnlyNonNullEqNullKeys)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto uint8_type = std::make_shared<DataTypeUInt8>();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({1, std::nullopt, 2}), nullable_int_type, probe_eq_key_name},
        {makeNullableInt32Column({std::nullopt, 10, 20}), nullable_int_type, probe_null_eq_key_name},
        {makeUInt8Column({1, 1, 0}), uint8_type, probe_filter_name},
    });

    probe_process_info.prepareForHashProbe(
        Names{probe_eq_key_name, probe_null_eq_key_name},
        std::vector<UInt8>{0, 1},
        probe_filter_name,
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        false,
        TiDB::TiDBCollators{},
        0);

    ASSERT_NE(probe_process_info.row_filter_map, nullptr);
    ASSERT_FALSE(probe_process_info.hash_join_data->key_columns[0]->isColumnNullable());
    ASSERT_TRUE(probe_process_info.hash_join_data->key_columns[1]->isColumnNullable());
    EXPECT_EQ((*probe_process_info.row_filter_map)[0], 0);
    EXPECT_EQ((*probe_process_info.row_filter_map)[1], 1);
    EXPECT_EQ((*probe_process_info.row_filter_map)[2], 1);
}

} // namespace DB::tests
